#include "streaming_compute/publisher.h"
#include <algorithm>

namespace streaming_compute {

Publisher::Publisher(const StreamConfig& config) : config_(config) {
}

Publisher::~Publisher() {
    stop();
}

bool Publisher::register_table(std::shared_ptr<StreamTable> table) {
    if (!table || !table->is_shared()) {
        return false;
    }

    std::unique_lock<std::shared_mutex> lock(tables_mutex_);
    
    const std::string& table_name = table->name();
    if (tables_.find(table_name) != tables_.end()) {
        return false; // Table already registered
    }

    tables_[table_name] = table;
    
    // Subscribe to table updates
    table->subscribe([this, table_name](const StreamMessage& message) {
        handle_table_message(table_name, message);
    });

    return true;
}

bool Publisher::unregister_table(const std::string& table_name) {
    std::unique_lock<std::shared_mutex> lock(tables_mutex_);
    
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return false;
    }

    // Remove all subscribers for this table
    {
        std::unique_lock<std::shared_mutex> sub_lock(subscribers_mutex_);
        auto topic_it = topic_subscribers_.find(table_name);
        if (topic_it != topic_subscribers_.end()) {
            total_subscribers_ -= topic_it->second.size();
            topic_subscribers_.erase(topic_it);
        }
    }

    tables_.erase(it);
    table_filter_columns_.erase(table_name);
    
    return true;
}

size_t Publisher::publish(const std::string& topic, const StreamMessage& message) {
    std::shared_lock<std::shared_mutex> lock(subscribers_mutex_);
    
    auto it = topic_subscribers_.find(topic);
    if (it == topic_subscribers_.end()) {
        return 0;
    }

    size_t delivered = 0;
    for (auto& subscriber : it->second) {
        if (subscriber) {
            // Add task to worker queue
            {
                std::lock_guard<std::mutex> task_lock(task_queue_mutex_);
                task_queue_.push([this, subscriber, message]() {
                    deliver_message(subscriber, message);
                });
            }
            task_condition_.notify_one();
            delivered++;
        }
    }

    total_messages_published_++;
    return delivered;
}

bool Publisher::add_subscriber(const std::string& topic,
                              const std::string& subscriber_id,
                              MessageHandler handler,
                              int64_t offset) {
    std::unique_lock<std::shared_mutex> lock(subscribers_mutex_);
    
    auto subscriber_info = std::make_shared<SubscriberInfo>(subscriber_id, topic, 
                                                           std::move(handler), offset);
    
    topic_subscribers_[topic].push_back(subscriber_info);
    total_subscribers_++;

    // If offset is specified, send historical data
    if (offset >= 0) {
        std::shared_lock<std::shared_mutex> tables_lock(tables_mutex_);
        auto table_it = tables_.find(topic);
        if (table_it != tables_.end()) {
            auto historical_batches = table_it->second->select(offset);
            int64_t current_offset = offset;
            
            for (const auto& batch : historical_batches) {
                StreamMessage historical_msg(current_offset, current_timestamp(), batch, topic);
                
                // Add to task queue
                {
                    std::lock_guard<std::mutex> task_lock(task_queue_mutex_);
                    task_queue_.push([this, subscriber_info, historical_msg]() {
                        deliver_message(subscriber_info, historical_msg);
                    });
                }
                task_condition_.notify_one();
                
                current_offset += batch->num_rows();
            }
        }
    }

    return true;
}

bool Publisher::remove_subscriber(const std::string& topic, const std::string& subscriber_id) {
    std::unique_lock<std::shared_mutex> lock(subscribers_mutex_);
    
    auto it = topic_subscribers_.find(topic);
    if (it == topic_subscribers_.end()) {
        return false;
    }

    auto& subscribers = it->second;
    auto subscriber_it = std::find_if(subscribers.begin(), subscribers.end(),
        [&subscriber_id](const std::shared_ptr<SubscriberInfo>& sub) {
            return sub && sub->id == subscriber_id;
        });

    if (subscriber_it != subscribers.end()) {
        subscribers.erase(subscriber_it);
        total_subscribers_--;
        
        // Remove topic if no subscribers left
        if (subscribers.empty()) {
            topic_subscribers_.erase(it);
        }
        
        return true;
    }

    return false;
}

bool Publisher::set_filter_column(const std::string& table_name, const std::string& column_name) {
    std::shared_lock<std::shared_mutex> lock(tables_mutex_);
    
    if (tables_.find(table_name) == tables_.end()) {
        return false;
    }

    table_filter_columns_[table_name] = column_name;
    return true;
}

std::vector<ConnectionStats> Publisher::get_connection_stats() const {
    std::vector<ConnectionStats> stats;
    
    std::shared_lock<std::shared_mutex> lock(subscribers_mutex_);
    
    for (const auto& topic_pair : topic_subscribers_) {
        const std::string& topic = topic_pair.first;
        const auto& subscribers = topic_pair.second;
        
        for (const auto& subscriber : subscribers) {
            if (subscriber) {
                ConnectionStats conn_stat;
                conn_stat.client = subscriber->id;
                conn_stat.queue_depth_limit = config_.max_queue_depth;
                conn_stat.queue_depth = subscriber->message_queue.size();
                conn_stat.tables = topic;
                
                stats.push_back(conn_stat);
            }
        }
    }
    
    return stats;
}

std::unordered_map<std::string, int64_t> Publisher::get_stats() const {
    std::unordered_map<std::string, int64_t> stats;
    
    stats["total_messages_published"] = total_messages_published_.load();
    stats["total_subscribers"] = total_subscribers_.load();
    stats["total_failed_deliveries"] = total_failed_deliveries_.load();
    stats["registered_tables"] = tables_.size();
    stats["active_topics"] = topic_subscribers_.size();
    stats["is_running"] = running_.load() ? 1 : 0;
    
    return stats;
}

bool Publisher::start() {
    if (running_) {
        return false;
    }

    stop_requested_ = false;
    running_ = true;

    // Start worker threads
    size_t num_workers = std::max(1u, std::thread::hardware_concurrency());
    for (size_t i = 0; i < num_workers; ++i) {
        worker_threads_.emplace_back(std::make_unique<std::thread>(&Publisher::worker_thread, this));
    }

    return true;
}

void Publisher::stop() {
    if (!running_) {
        return;
    }

    stop_requested_ = true;
    running_ = false;

    // Notify all worker threads
    task_condition_.notify_all();

    // Wait for worker threads to finish
    for (auto& thread : worker_threads_) {
        if (thread && thread->joinable()) {
            thread->join();
        }
    }
    worker_threads_.clear();

    // Clear remaining tasks
    std::lock_guard<std::mutex> lock(task_queue_mutex_);
    while (!task_queue_.empty()) {
        task_queue_.pop();
    }
}

void Publisher::worker_thread() {
    while (!stop_requested_) {
        std::unique_lock<std::mutex> lock(task_queue_mutex_);
        
        task_condition_.wait(lock, [this] {
            return !task_queue_.empty() || stop_requested_;
        });

        if (stop_requested_) {
            break;
        }

        if (!task_queue_.empty()) {
            auto task = task_queue_.front();
            task_queue_.pop();
            lock.unlock();

            try {
                task();
            } catch (const std::exception& e) {
                // Log error
                total_failed_deliveries_++;
            }
        }
    }
}

void Publisher::deliver_message(std::shared_ptr<SubscriberInfo> subscriber, const StreamMessage& message) {
    if (!subscriber) {
        return;
    }

    try {
        // Apply filtering if configured
        std::shared_lock<std::shared_mutex> tables_lock(tables_mutex_);
        auto filter_it = table_filter_columns_.find(message.topic);
        if (filter_it != table_filter_columns_.end()) {
            if (!apply_filter(message, filter_it->second)) {
                return; // Message filtered out
            }
        }
        tables_lock.unlock();

        // Check queue depth
        {
            std::lock_guard<std::mutex> queue_lock(subscriber->queue_mutex);
            if (subscriber->message_queue.size() >= config_.max_queue_depth) {
                total_failed_deliveries_++;
                return;
            }
            subscriber->message_queue.push(message);
        }

        // Call handler
        subscriber->handler(message);
        subscriber->messages_sent++;
        subscriber->last_activity = std::chrono::system_clock::now();

    } catch (const std::exception& e) {
        subscriber->messages_failed++;
        total_failed_deliveries_++;
    }
}

bool Publisher::apply_filter(const StreamMessage& message, const std::string& filter_column) const {
    // This is a simplified filter implementation
    // In practice, you'd need more sophisticated filtering based on column values
    return true;
}

void Publisher::cleanup_inactive_subscribers() {
    std::unique_lock<std::shared_mutex> lock(subscribers_mutex_);
    
    auto now = std::chrono::system_clock::now();
    auto timeout = std::chrono::minutes(30); // 30 minute timeout
    
    for (auto& topic_pair : topic_subscribers_) {
        auto& subscribers = topic_pair.second;
        
        subscribers.erase(
            std::remove_if(subscribers.begin(), subscribers.end(),
                [now, timeout](const std::shared_ptr<SubscriberInfo>& sub) {
                    return sub && (now - sub->last_activity) > timeout;
                }),
            subscribers.end()
        );
    }
}

void Publisher::handle_table_message(const std::string& table_name, const StreamMessage& message) {
    // Publish message to all subscribers of this table
    publish(table_name, message);
}

} // namespace streaming_compute