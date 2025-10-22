#include "streaming_compute/subscriber.h"
#include <algorithm>

namespace streaming_compute {

Subscriber::Subscriber(const std::string& subscriber_id, const StreamConfig& config)
    : subscriber_id_(subscriber_id), config_(config) {
}

Subscriber::~Subscriber() {
    stop();
}

std::string Subscriber::subscribe_table(const std::string& server,
                                       const std::string& table_name,
                                       const std::string& action_name,
                                       int64_t offset,
                                       MessageHandler handler,
                                       bool msg_as_table,
                                       size_t batch_size,
                                       double throttle,
                                       bool reconnect) {
    std::string topic = generate_topic(server, table_name, action_name);
    
    std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
    
    // Check if subscription already exists
    if (subscriptions_.find(topic) != subscriptions_.end()) {
        return ""; // Subscription already exists
    }

    auto subscription = std::make_shared<Subscription>(
        topic, server, table_name, action_name, offset,
        std::move(handler), BatchHandler{}, msg_as_table,
        batch_size, throttle, reconnect
    );

    subscriptions_[topic] = subscription;
    
    // Set initial offset
    if (offset >= 0) {
        std::lock_guard<std::mutex> offset_lock(offsets_mutex_);
        topic_offsets_[topic] = offset;
    }

    // If running, start processing this subscription immediately
    if (running_) {
        std::lock_guard<std::mutex> task_lock(task_queue_mutex_);
        task_queue_.push([this, subscription]() {
            process_subscription(subscription);
        });
        task_condition_.notify_one();
    }

    return topic;
}

std::string Subscriber::subscribe_table_batch(const std::string& server,
                                             const std::string& table_name,
                                             const std::string& action_name,
                                             int64_t offset,
                                             BatchHandler handler,
                                             size_t batch_size,
                                             double throttle,
                                             bool reconnect) {
    std::string topic = generate_topic(server, table_name, action_name);
    
    std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
    
    // Check if subscription already exists
    if (subscriptions_.find(topic) != subscriptions_.end()) {
        return ""; // Subscription already exists
    }

    auto subscription = std::make_shared<Subscription>(
        topic, server, table_name, action_name, offset,
        MessageHandler{}, std::move(handler), false,
        batch_size, throttle, reconnect
    );

    subscriptions_[topic] = subscription;
    
    // Set initial offset
    if (offset >= 0) {
        std::lock_guard<std::mutex> offset_lock(offsets_mutex_);
        topic_offsets_[topic] = offset;
    }

    // If running, start processing this subscription immediately
    if (running_) {
        std::lock_guard<std::mutex> task_lock(task_queue_mutex_);
        task_queue_.push([this, subscription]() {
            process_subscription(subscription);
        });
        task_condition_.notify_one();
    }

    return topic;
}

bool Subscriber::unsubscribe_table(const std::string& server,
                                  const std::string& table_name,
                                  const std::string& action_name,
                                  bool remove_offset) {
    std::string topic = generate_topic(server, table_name, action_name);
    
    std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
    
    auto it = subscriptions_.find(topic);
    if (it == subscriptions_.end()) {
        return false;
    }

    // Mark subscription as inactive
    it->second->active = false;
    
    // Remove subscription
    subscriptions_.erase(it);

    if (remove_offset) {
        std::lock_guard<std::mutex> offset_lock(offsets_mutex_);
        topic_offsets_.erase(topic);
    }

    return true;
}

std::vector<SubscriptionStats> Subscriber::get_subscription_stats() const {
    std::vector<SubscriptionStats> stats;
    
    std::shared_lock<std::shared_mutex> lock(subscriptions_mutex_);
    
    for (const auto& sub_pair : subscriptions_) {
        const auto& subscription = sub_pair.second;
        
        SubscriptionStats stat;
        stat.publisher = subscription->server.empty() ? "local" : subscription->server;
        stat.cum_msg_count = subscription->messages_received.load();
        stat.cum_msg_latency = 0.0; // Would need to track timing
        stat.last_msg_latency = 0.0; // Would need to track timing
        stat.last_update = subscription->last_message_time;
        
        stats.push_back(stat);
    }
    
    return stats;
}

std::vector<WorkerStats> Subscriber::get_worker_stats() const {
    std::vector<WorkerStats> stats;
    
    std::shared_lock<std::shared_mutex> lock(subscriptions_mutex_);
    
    int worker_id = 0;
    for (const auto& sub_pair : subscriptions_) {
        const auto& subscription = sub_pair.second;
        
        WorkerStats stat;
        stat.worker_id = worker_id++;
        stat.topic = subscription->topic;
        stat.queue_depth_limit = config_.max_queue_depth;
        stat.queue_depth = subscription->message_batch.size();
        stat.processed_msg_count = subscription->messages_processed.load();
        stat.failed_msg_count = subscription->messages_failed.load();
        stat.last_error_msg = ""; // Would need to track errors
        
        stats.push_back(stat);
    }
    
    return stats;
}

int64_t Subscriber::get_processed_offset(const std::string& topic) const {
    std::lock_guard<std::mutex> lock(offsets_mutex_);
    
    auto it = topic_offsets_.find(topic);
    if (it != topic_offsets_.end()) {
        return it->second;
    }
    
    return -1;
}

void Subscriber::set_processed_offset(const std::string& topic, int64_t offset) {
    std::lock_guard<std::mutex> lock(offsets_mutex_);
    topic_offsets_[topic] = offset;
}

void Subscriber::remove_topic_offset(const std::string& topic) {
    std::lock_guard<std::mutex> lock(offsets_mutex_);
    topic_offsets_.erase(topic);
}

bool Subscriber::start() {
    if (running_) {
        return false;
    }

    stop_requested_ = false;
    running_ = true;

    // Start worker threads
    size_t num_workers = std::max(1u, std::thread::hardware_concurrency());
    for (size_t i = 0; i < num_workers; ++i) {
        worker_threads_.emplace_back(std::make_unique<std::thread>(&Subscriber::worker_thread, this));
    }

    // Start processing existing subscriptions
    {
        std::shared_lock<std::shared_mutex> lock(subscriptions_mutex_);
        for (const auto& sub_pair : subscriptions_) {
            std::lock_guard<std::mutex> task_lock(task_queue_mutex_);
            task_queue_.push([this, subscription = sub_pair.second]() {
                process_subscription(subscription);
            });
        }
    }
    task_condition_.notify_all();

    return true;
}

void Subscriber::stop() {
    if (!running_) {
        return;
    }

    stop_requested_ = true;
    running_ = false;

    // Mark all subscriptions as inactive
    {
        std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
        for (auto& sub_pair : subscriptions_) {
            sub_pair.second->active = false;
        }
    }

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

void Subscriber::worker_thread() {
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
                total_messages_failed_++;
            }
        }
    }
}

void Subscriber::process_subscription(std::shared_ptr<Subscription> subscription) {
    if (!subscription || !subscription->active) {
        return;
    }

    // This is a simplified implementation
    // In a real system, you'd connect to the publisher and receive messages
    
    // For now, we'll simulate periodic batch processing
    while (subscription->active && !stop_requested_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(
            static_cast<int>(subscription->throttle * 1000)));

        // Process any accumulated batch
        if (subscription->batch_handler && !subscription->message_batch.empty()) {
            process_batch(subscription);
        }

        // Check for reconnection if needed
        if (subscription->reconnect) {
            // Attempt reconnection logic would go here
        }
    }
}

void Subscriber::handle_message(std::shared_ptr<Subscription> subscription, const StreamMessage& message) {
    if (!subscription || !subscription->active) {
        return;
    }

    subscription->messages_received++;
    subscription->last_message_time = std::chrono::system_clock::now();
    total_messages_received_++;

    try {
        if (subscription->message_handler) {
            // Handle individual message
            subscription->message_handler(message);
            subscription->messages_processed++;
            total_messages_processed_++;
            
            // Update offset
            set_processed_offset(subscription->topic, message.offset);
            
        } else if (subscription->batch_handler) {
            // Add to batch
            std::lock_guard<std::mutex> batch_lock(subscription->batch_mutex);
            subscription->message_batch.push_back(message);
            
            // Process batch if it's full or throttle time has passed
            auto now = std::chrono::system_clock::now();
            auto time_since_last_batch = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - subscription->last_batch_time).count();
            
            if (subscription->message_batch.size() >= subscription->batch_size ||
                time_since_last_batch >= subscription->throttle * 1000) {
                process_batch(subscription);
            }
        }
        
        update_statistics(subscription, true);
        
    } catch (const std::exception& e) {
        subscription->messages_failed++;
        total_messages_failed_++;
        update_statistics(subscription, false);
    }
}

void Subscriber::process_batch(std::shared_ptr<Subscription> subscription) {
    if (!subscription || !subscription->batch_handler) {
        return;
    }

    std::lock_guard<std::mutex> batch_lock(subscription->batch_mutex);
    
    if (subscription->message_batch.empty()) {
        return;
    }

    try {
        subscription->batch_handler(subscription->message_batch);
        
        // Update statistics
        subscription->messages_processed += subscription->message_batch.size();
        total_messages_processed_ += subscription->message_batch.size();
        
        // Update offset to the last message in the batch
        if (!subscription->message_batch.empty()) {
            const auto& last_message = subscription->message_batch.back();
            set_processed_offset(subscription->topic, last_message.offset);
        }
        
        update_statistics(subscription, true);
        
    } catch (const std::exception& e) {
        subscription->messages_failed += subscription->message_batch.size();
        total_messages_failed_ += subscription->message_batch.size();
        update_statistics(subscription, false);
    }

    subscription->message_batch.clear();
    subscription->last_batch_time = std::chrono::system_clock::now();
}

void Subscriber::attempt_reconnection(std::shared_ptr<Subscription> subscription) {
    if (!subscription || !subscription->reconnect) {
        return;
    }

    // Simplified reconnection logic
    if (connect_to_publisher(subscription->server)) {
        total_reconnections_++;
        // Restart subscription processing
        std::lock_guard<std::mutex> task_lock(task_queue_mutex_);
        task_queue_.push([this, subscription]() {
            process_subscription(subscription);
        });
        task_condition_.notify_one();
    }
}

bool Subscriber::connect_to_publisher(const std::string& server) {
    // Simplified connection logic
    // In a real implementation, this would establish network connections
    return true;
}

void Subscriber::update_statistics(std::shared_ptr<Subscription> subscription, bool success) {
    // Update timing and other statistics
    subscription->last_message_time = std::chrono::system_clock::now();
}

std::string Subscriber::generate_topic(const std::string& server, const std::string& table_name,
                                      const std::string& action_name) const {
    std::string node = server.empty() ? "local" : server;
    if (action_name.empty()) {
        return node + "/" + table_name;
    }
    return node + "/" + table_name + "/" + action_name;
}

} // namespace streaming_compute