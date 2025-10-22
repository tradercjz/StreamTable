#include "streaming_compute/monitoring.h"
#include <sstream>
#include <iomanip>
#include <fstream>
#include <thread>

#ifdef __linux__
#include <sys/sysinfo.h>
#include <unistd.h>
#endif

namespace streaming_compute {

StreamingMonitor::StreamingMonitor(const MonitoringConfig& config) : config_(config) {
}

StreamingMonitor::~StreamingMonitor() {
    stop();
}

bool StreamingMonitor::start() {
    if (active_) {
        return false;
    }

    shutdown_requested_ = false;
    active_ = true;

    // Start monitoring thread
    monitoring_thread_ = std::make_unique<std::thread>(&StreamingMonitor::monitoring_worker, this);

    return true;
}

void StreamingMonitor::stop() {
    if (!active_) {
        return;
    }

    shutdown_requested_ = true;
    active_ = false;

    if (monitoring_thread_ && monitoring_thread_->joinable()) {
        monitoring_thread_->join();
    }
}

void StreamingMonitor::register_table(std::shared_ptr<StreamTable> table) {
    if (!table) {
        return;
    }

    std::unique_lock<std::shared_mutex> lock(components_mutex_);
    monitored_tables_.push_back(table);
}

void StreamingMonitor::register_publisher(std::shared_ptr<Publisher> publisher) {
    if (!publisher) {
        return;
    }

    std::unique_lock<std::shared_mutex> lock(components_mutex_);
    monitored_publishers_.push_back(publisher);
}

void StreamingMonitor::register_subscriber(std::shared_ptr<Subscriber> subscriber) {
    if (!subscriber) {
        return;
    }

    std::unique_lock<std::shared_mutex> lock(components_mutex_);
    monitored_subscribers_.push_back(subscriber);
}

void StreamingMonitor::register_engine(std::shared_ptr<StreamEngine> engine) {
    if (!engine) {
        return;
    }

    std::unique_lock<std::shared_mutex> lock(components_mutex_);
    monitored_engines_.push_back(engine);
}

std::unordered_map<std::string, std::vector<std::unordered_map<std::string, std::string>>> 
StreamingMonitor::get_streaming_stat() const {
    std::unordered_map<std::string, std::vector<std::unordered_map<std::string, std::string>>> result;

    // pubConns
    auto pub_conns = get_pub_conns();
    std::vector<std::unordered_map<std::string, std::string>> pub_conns_data;
    for (const auto& conn : pub_conns) {
        std::unordered_map<std::string, std::string> conn_data;
        conn_data["client"] = conn.client;
        conn_data["queueDepthLimit"] = std::to_string(conn.queue_depth_limit);
        conn_data["queueDepth"] = std::to_string(conn.queue_depth);
        conn_data["tables"] = conn.tables;
        pub_conns_data.push_back(conn_data);
    }
    result["pubConns"] = pub_conns_data;

    // subConns
    auto sub_conns = get_sub_conns();
    std::vector<std::unordered_map<std::string, std::string>> sub_conns_data;
    for (const auto& conn : sub_conns) {
        std::unordered_map<std::string, std::string> conn_data;
        conn_data["publisher"] = conn.publisher;
        conn_data["cumMsgCount"] = std::to_string(conn.cum_msg_count);
        conn_data["cumMsgLatency"] = std::to_string(conn.cum_msg_latency);
        conn_data["lastMsgLatency"] = std::to_string(conn.last_msg_latency);
        conn_data["lastUpdate"] = format_timestamp(conn.last_update);
        sub_conns_data.push_back(conn_data);
    }
    result["subConns"] = sub_conns_data;

    // subWorkers
    auto sub_workers = get_sub_workers();
    std::vector<std::unordered_map<std::string, std::string>> sub_workers_data;
    for (const auto& worker : sub_workers) {
        std::unordered_map<std::string, std::string> worker_data;
        worker_data["workerId"] = std::to_string(worker.worker_id);
        worker_data["topic"] = worker.topic;
        worker_data["queueDepthLimit"] = std::to_string(worker.queue_depth_limit);
        worker_data["queueDepth"] = std::to_string(worker.queue_depth);
        worker_data["processedMsgCount"] = std::to_string(worker.processed_msg_count);
        worker_data["failedMsgCount"] = std::to_string(worker.failed_msg_count);
        worker_data["lastErrMsg"] = worker.last_error_msg;
        sub_workers_data.push_back(worker_data);
    }
    result["subWorkers"] = sub_workers_data;

    // pubTables
    result["pubTables"] = get_pub_tables();

    return result;
}

std::unordered_map<std::string, std::vector<std::unordered_map<std::string, std::string>>> 
StreamingMonitor::get_stream_engine_stat() const {
    std::unordered_map<std::string, std::vector<std::unordered_map<std::string, std::string>>> result;

    std::shared_lock<std::shared_mutex> components_lock(components_mutex_);
    
    // Group engines by type
    std::unordered_map<std::string, std::vector<std::shared_ptr<StreamEngine>>> engines_by_type;
    
    for (auto weak_engine : monitored_engines_) {
        if (auto engine = weak_engine.lock()) {
            std::string engine_type = "StreamEngine"; // Base type
            // In practice, you'd determine the actual engine type
            engines_by_type[engine_type].push_back(engine);
        }
    }
    
    components_lock.unlock();

    // Create statistics for each engine type
    for (const auto& type_pair : engines_by_type) {
        const std::string& engine_type = type_pair.first;
        const auto& engines = type_pair.second;
        
        std::vector<std::unordered_map<std::string, std::string>> engine_stats;
        
        for (const auto& engine : engines) {
            std::unordered_map<std::string, std::string> stats;
            
            auto engine_stats_map = engine->get_stats();
            for (const auto& stat_pair : engine_stats_map) {
                stats[stat_pair.first] = std::to_string(stat_pair.second);
            }
            
            stats["name"] = engine->name();
            stats["keyColumn"] = engine->key_column();
            stats["isActive"] = engine->is_active() ? "true" : "false";
            
            engine_stats.push_back(stats);
        }
        
        result[engine_type] = engine_stats;
    }

    return result;
}

std::vector<ConnectionStats> StreamingMonitor::get_pub_conns() const {
    std::vector<ConnectionStats> stats;
    
    std::shared_lock<std::shared_mutex> lock(components_mutex_);
    
    for (auto weak_publisher : monitored_publishers_) {
        if (auto publisher = weak_publisher.lock()) {
            auto pub_stats = publisher->get_connection_stats();
            stats.insert(stats.end(), pub_stats.begin(), pub_stats.end());
        }
    }
    
    return stats;
}

std::vector<SubscriptionStats> StreamingMonitor::get_sub_conns() const {
    std::vector<SubscriptionStats> stats;
    
    std::shared_lock<std::shared_mutex> lock(components_mutex_);
    
    for (auto weak_subscriber : monitored_subscribers_) {
        if (auto subscriber = weak_subscriber.lock()) {
            auto sub_stats = subscriber->get_subscription_stats();
            stats.insert(stats.end(), sub_stats.begin(), sub_stats.end());
        }
    }
    
    return stats;
}

std::vector<WorkerStats> StreamingMonitor::get_sub_workers() const {
    std::vector<WorkerStats> stats;
    
    std::shared_lock<std::shared_mutex> lock(components_mutex_);
    
    for (auto weak_subscriber : monitored_subscribers_) {
        if (auto subscriber = weak_subscriber.lock()) {
            auto worker_stats = subscriber->get_worker_stats();
            stats.insert(stats.end(), worker_stats.begin(), worker_stats.end());
        }
    }
    
    return stats;
}

std::vector<std::unordered_map<std::string, std::string>> StreamingMonitor::get_pub_tables() const {
    std::vector<std::unordered_map<std::string, std::string>> result;
    
    std::shared_lock<std::shared_mutex> lock(components_mutex_);
    
    for (auto weak_table : monitored_tables_) {
        if (auto table = weak_table.lock()) {
            if (table->is_shared()) {
                std::unordered_map<std::string, std::string> table_info;
                table_info["tableName"] = table->name();
                table_info["subscriber"] = "local"; // Simplified
                table_info["msgOffset"] = std::to_string(table->current_offset());
                table_info["actions"] = "monitoring"; // Simplified
                result.push_back(table_info);
            }
        }
    }
    
    return result;
}

std::unordered_map<std::string, double> StreamingMonitor::get_performance_metrics() const {
    std::unordered_map<std::string, double> metrics;
    
    metrics["cpu_usage"] = cpu_usage_.load();
    metrics["memory_usage"] = memory_usage_.load();
    metrics["total_threads"] = static_cast<double>(total_threads_.load());
    
    // Add component-specific metrics
    std::shared_lock<std::shared_mutex> stats_lock(stats_mutex_);
    
    double total_messages_processed = 0;
    double total_messages_failed = 0;
    double total_queue_depth = 0;
    
    for (const auto& comp_pair : component_stats_) {
        const auto& stats = comp_pair.second;
        std::lock_guard<std::mutex> comp_lock(stats->stats_mutex);
        
        auto it = stats->current_stats.find("processed_messages");
        if (it != stats->current_stats.end()) {
            total_messages_processed += it->second;
        }
        
        it = stats->current_stats.find("failed_messages");
        if (it != stats->current_stats.end()) {
            total_messages_failed += it->second;
        }
        
        it = stats->current_stats.find("queue_depth");
        if (it != stats->current_stats.end()) {
            total_queue_depth += it->second;
        }
    }
    
    metrics["total_messages_processed"] = total_messages_processed;
    metrics["total_messages_failed"] = total_messages_failed;
    metrics["total_queue_depth"] = total_queue_depth;
    
    if (total_messages_processed > 0) {
        metrics["error_rate"] = total_messages_failed / total_messages_processed;
    } else {
        metrics["error_rate"] = 0.0;
    }
    
    return metrics;
}

std::vector<std::string> StreamingMonitor::get_active_alerts() const {
    std::vector<std::string> active_alerts;
    
    std::lock_guard<std::mutex> lock(alerts_mutex_);
    
    for (const auto& alert : alerts_) {
        if (alert.active) {
            std::ostringstream oss;
            oss << "[" << alert.severity << "] " 
                << alert.component_type << ":" << alert.component_name 
                << " - " << alert.message 
                << " (" << format_timestamp(alert.timestamp) << ")";
            active_alerts.push_back(oss.str());
        }
    }
    
    return active_alerts;
}

std::vector<std::unordered_map<std::string, double>> 
StreamingMonitor::get_historical_stats(const std::string& component_type,
                                      const std::string& component_name,
                                      size_t minutes_back) const {
    std::vector<std::unordered_map<std::string, double>> result;
    
    std::string key = component_type + ":" + component_name;
    
    std::shared_lock<std::shared_mutex> stats_lock(stats_mutex_);
    auto it = component_stats_.find(key);
    if (it == component_stats_.end()) {
        return result;
    }
    
    const auto& stats = it->second;
    std::lock_guard<std::mutex> comp_lock(stats->stats_mutex);
    
    auto cutoff_time = std::chrono::system_clock::now() - std::chrono::minutes(minutes_back);
    
    for (const auto& history_entry : stats->history) {
        if (history_entry.first >= cutoff_time) {
            result.push_back(history_entry.second);
        }
    }
    
    return result;
}

void StreamingMonitor::monitoring_worker() {
    while (!shutdown_requested_) {
        collect_statistics();
        check_alerts();
        cleanup_old_history();
        
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.stats_collection_interval_ms));
    }
}

void StreamingMonitor::collect_statistics() {
    // Collect system statistics
    collect_system_stats();
    
    std::shared_lock<std::shared_mutex> lock(components_mutex_);
    
    // Collect table statistics
    for (auto weak_table : monitored_tables_) {
        if (auto table = weak_table.lock()) {
            collect_table_stats(table);
        }
    }
    
    // Collect publisher statistics
    for (auto weak_publisher : monitored_publishers_) {
        if (auto publisher = weak_publisher.lock()) {
            collect_publisher_stats(publisher);
        }
    }
    
    // Collect subscriber statistics
    for (auto weak_subscriber : monitored_subscribers_) {
        if (auto subscriber = weak_subscriber.lock()) {
            collect_subscriber_stats(subscriber);
        }
    }
    
    // Collect engine statistics
    for (auto weak_engine : monitored_engines_) {
        if (auto engine = weak_engine.lock()) {
            collect_engine_stats(engine);
        }
    }
}

void StreamingMonitor::collect_table_stats(std::shared_ptr<StreamTable> table) {
    auto stats_map = table->get_stats();
    
    std::unordered_map<std::string, double> stats;
    for (const auto& pair : stats_map) {
        stats[pair.first] = static_cast<double>(pair.second);
    }
    
    update_component_stats(table->name(), "table", stats);
}

void StreamingMonitor::collect_publisher_stats(std::shared_ptr<Publisher> publisher) {
    auto stats_map = publisher->get_stats();
    
    std::unordered_map<std::string, double> stats;
    for (const auto& pair : stats_map) {
        stats[pair.first] = static_cast<double>(pair.second);
    }
    
    update_component_stats("publisher", "publisher", stats);
}

void StreamingMonitor::collect_subscriber_stats(std::shared_ptr<Subscriber> subscriber) {
    // Subscriber doesn't have a direct get_stats method in our implementation
    // This would be implemented based on the specific subscriber interface
    std::unordered_map<std::string, double> stats;
    stats["is_running"] = subscriber->is_running() ? 1.0 : 0.0;
    
    update_component_stats("subscriber", "subscriber", stats);
}

void StreamingMonitor::collect_engine_stats(std::shared_ptr<StreamEngine> engine) {
    auto stats_map = engine->get_stats();
    
    std::unordered_map<std::string, double> stats;
    for (const auto& pair : stats_map) {
        stats[pair.first] = static_cast<double>(pair.second);
    }
    
    update_component_stats(engine->name(), "engine", stats);
}

void StreamingMonitor::collect_system_stats() {
    cpu_usage_ = get_cpu_usage();
    memory_usage_ = get_memory_usage();
    total_threads_ = get_thread_count();
}

void StreamingMonitor::update_component_stats(const std::string& component_name,
                                             const std::string& component_type,
                                             const std::unordered_map<std::string, double>& stats) {
    std::string key = component_type + ":" + component_name;
    
    std::unique_lock<std::shared_mutex> stats_lock(stats_mutex_);
    
    auto it = component_stats_.find(key);
    if (it == component_stats_.end()) {
        component_stats_[key] = std::make_unique<ComponentStats>(component_name, component_type);
        it = component_stats_.find(key);
    }
    
    auto& comp_stats = it->second;
    stats_lock.unlock();
    
    std::lock_guard<std::mutex> comp_lock(comp_stats->stats_mutex);
    
    comp_stats->current_stats = stats;
    
    // Add to history
    auto now = std::chrono::system_clock::now();
    comp_stats->history.emplace_back(now, stats);
    
    // Limit history size
    auto cutoff_time = now - std::chrono::minutes(config_.history_retention_minutes);
    while (!comp_stats->history.empty() && comp_stats->history.front().first < cutoff_time) {
        comp_stats->history.pop_front();
    }
}

void StreamingMonitor::check_alerts() {
    if (!config_.enable_alerts) {
        return;
    }
    
    // Check system alerts
    double cpu = cpu_usage_.load();
    if (cpu > config_.cpu_threshold) {
        add_alert("system", "system", 
                 "High CPU usage: " + std::to_string(cpu) + "%", "warning");
    } else {
        clear_alert("system", "system", "High CPU usage");
    }
    
    double memory = memory_usage_.load();
    if (memory > config_.memory_threshold) {
        add_alert("system", "system", 
                 "High memory usage: " + std::to_string(memory) + "%", "warning");
    } else {
        clear_alert("system", "system", "High memory usage");
    }
    
    // Check component alerts
    std::shared_lock<std::shared_mutex> stats_lock(stats_mutex_);
    
    for (const auto& comp_pair : component_stats_) {
        const auto& stats = comp_pair.second;
        std::lock_guard<std::mutex> comp_lock(stats->stats_mutex);
        
        // Check queue depth
        auto it = stats->current_stats.find("queue_depth");
        if (it != stats->current_stats.end() && it->second > config_.queue_depth_threshold) {
            add_alert(stats->name, stats->type,
                     "High queue depth: " + std::to_string(static_cast<int>(it->second)), "warning");
        } else {
            clear_alert(stats->name, stats->type, "High queue depth");
        }
        
        // Check error rate
        auto processed_it = stats->current_stats.find("processed_messages");
        auto failed_it = stats->current_stats.find("failed_messages");
        
        if (processed_it != stats->current_stats.end() && 
            failed_it != stats->current_stats.end() &&
            processed_it->second > 0) {
            
            double error_rate = failed_it->second / processed_it->second;
            if (error_rate > 0.1) { // 10% error rate threshold
                add_alert(stats->name, stats->type,
                         "High error rate: " + std::to_string(error_rate * 100) + "%", "error");
            } else {
                clear_alert(stats->name, stats->type, "High error rate");
            }
        }
    }
}

void StreamingMonitor::add_alert(const std::string& component_name,
                                const std::string& component_type,
                                const std::string& message,
                                const std::string& severity) {
    std::lock_guard<std::mutex> lock(alerts_mutex_);
    
    // Check if alert already exists
    for (auto& alert : alerts_) {
        if (alert.component_name == component_name &&
            alert.component_type == component_type &&
            alert.message.find(message.substr(0, message.find(':'))) != std::string::npos) {
            alert.active = true;
            alert.timestamp = std::chrono::system_clock::now();
            return;
        }
    }
    
    // Add new alert
    alerts_.emplace_back(component_name, component_type, message, severity);
}

void StreamingMonitor::clear_alert(const std::string& component_name,
                                  const std::string& component_type,
                                  const std::string& message) {
    std::lock_guard<std::mutex> lock(alerts_mutex_);
    
    for (auto& alert : alerts_) {
        if (alert.component_name == component_name &&
            alert.component_type == component_type &&
            alert.message.find(message) != std::string::npos) {
            alert.active = false;
        }
    }
}

void StreamingMonitor::cleanup_old_history() {
    // This is handled in update_component_stats
}

double StreamingMonitor::get_cpu_usage() const {
#ifdef __linux__
    static long long last_total_time = 0;
    static long long last_idle_time = 0;
    
    std::ifstream proc_stat("/proc/stat");
    if (!proc_stat.is_open()) {
        return 0.0;
    }
    
    std::string line;
    std::getline(proc_stat, line);
    
    std::istringstream iss(line);
    std::string cpu;
    long long user, nice, system, idle, iowait, irq, softirq, steal;
    
    iss >> cpu >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal;
    
    long long total_time = user + nice + system + idle + iowait + irq + softirq + steal;
    long long idle_time = idle + iowait;
    
    if (last_total_time != 0) {
        long long total_diff = total_time - last_total_time;
        long long idle_diff = idle_time - last_idle_time;
        
        if (total_diff > 0) {
            double cpu_usage = 100.0 * (1.0 - static_cast<double>(idle_diff) / total_diff);
            last_total_time = total_time;
            last_idle_time = idle_time;
            return cpu_usage;
        }
    }
    
    last_total_time = total_time;
    last_idle_time = idle_time;
#endif
    
    return 0.0; // Fallback for non-Linux systems
}

double StreamingMonitor::get_memory_usage() const {
#ifdef __linux__
    struct sysinfo info;
    if (sysinfo(&info) == 0) {
        double total_mem = info.totalram * info.mem_unit;
        double free_mem = info.freeram * info.mem_unit;
        double used_mem = total_mem - free_mem;
        return (used_mem / total_mem) * 100.0;
    }
#endif
    
    return 0.0; // Fallback for non-Linux systems
}

int64_t StreamingMonitor::get_thread_count() const {
    return std::thread::hardware_concurrency();
}

std::string StreamingMonitor::format_timestamp(const std::chrono::system_clock::time_point& tp) const {
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    std::ostringstream oss;
    oss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

std::string StreamingMonitor::format_duration(const std::chrono::system_clock::duration& duration) const {
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
    auto minutes = seconds / 60;
    auto hours = minutes / 60;
    
    std::ostringstream oss;
    if (hours > 0) {
        oss << hours << "h " << (minutes % 60) << "m " << (seconds % 60) << "s";
    } else if (minutes > 0) {
        oss << minutes << "m " << (seconds % 60) << "s";
    } else {
        oss << seconds << "s";
    }
    
    return oss.str();
}

} // namespace streaming_compute