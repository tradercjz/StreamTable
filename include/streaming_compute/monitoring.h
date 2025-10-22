#pragma once

#include "common.h"
#include "stream_table.h"
#include "publisher.h"
#include "subscriber.h"
#include "stream_engine.h"

namespace streaming_compute {

/**
 * StreamingMonitor - Comprehensive monitoring system for streaming components
 * 
 * Features:
 * - Real-time statistics collection
 * - Performance metrics
 * - Health monitoring
 * - Alert generation
 * - Historical data tracking
 */
class StreamingMonitor {
public:
    struct MonitoringConfig {
        bool enable_detailed_stats = true;
        size_t stats_collection_interval_ms = 1000; // 1 second
        size_t history_retention_minutes = 60; // 1 hour
        bool enable_alerts = true;
        double cpu_threshold = 80.0; // CPU usage threshold
        double memory_threshold = 80.0; // Memory usage threshold
        size_t queue_depth_threshold = 10000; // Queue depth threshold
    };

    /**
     * Constructor
     * @param config Monitoring configuration
     */
    explicit StreamingMonitor(const MonitoringConfig& config);

    /**
     * Destructor
     */
    ~StreamingMonitor();

    /**
     * Start monitoring
     * @return true if successful, false otherwise
     */
    bool start();

    /**
     * Stop monitoring
     */
    void stop();

    /**
     * Register a stream table for monitoring
     * @param table Stream table to monitor
     */
    void register_table(std::shared_ptr<StreamTable> table);

    /**
     * Register a publisher for monitoring
     * @param publisher Publisher to monitor
     */
    void register_publisher(std::shared_ptr<Publisher> publisher);

    /**
     * Register a subscriber for monitoring
     * @param subscriber Subscriber to monitor
     */
    void register_subscriber(std::shared_ptr<Subscriber> subscriber);

    /**
     * Register a stream engine for monitoring
     * @param engine Stream engine to monitor
     */
    void register_engine(std::shared_ptr<StreamEngine> engine);

    /**
     * Get streaming statistics (equivalent to DolphinDB's getStreamingStat)
     * @return Dictionary containing all streaming statistics
     */
    std::unordered_map<std::string, std::vector<std::unordered_map<std::string, std::string>>> get_streaming_stat() const;

    /**
     * Get stream engine statistics
     * @return Dictionary containing engine statistics by type
     */
    std::unordered_map<std::string, std::vector<std::unordered_map<std::string, std::string>>> get_stream_engine_stat() const;

    /**
     * Get connection statistics
     * @return Vector of connection statistics
     */
    std::vector<ConnectionStats> get_pub_conns() const;

    /**
     * Get subscription statistics
     * @return Vector of subscription statistics
     */
    std::vector<SubscriptionStats> get_sub_conns() const;

    /**
     * Get worker statistics
     * @return Vector of worker statistics
     */
    std::vector<WorkerStats> get_sub_workers() const;

    /**
     * Get table publication statistics
     * @return Vector of table publication statistics
     */
    std::vector<std::unordered_map<std::string, std::string>> get_pub_tables() const;

    /**
     * Get system performance metrics
     * @return Map of performance metrics
     */
    std::unordered_map<std::string, double> get_performance_metrics() const;

    /**
     * Get active alerts
     * @return Vector of active alerts
     */
    std::vector<std::string> get_active_alerts() const;

    /**
     * Get historical statistics
     * @param component_type Component type ("table", "publisher", "subscriber", "engine")
     * @param component_name Component name
     * @param minutes_back Minutes of history to retrieve
     * @return Vector of historical statistics
     */
    std::vector<std::unordered_map<std::string, double>> get_historical_stats(
        const std::string& component_type,
        const std::string& component_name,
        size_t minutes_back = 10) const;

    /**
     * Check if monitoring is active
     * @return true if active, false otherwise
     */
    bool is_active() const { return active_; }

private:
    struct ComponentStats {
        std::string name;
        std::string type;
        std::unordered_map<std::string, double> current_stats;
        std::deque<std::pair<std::chrono::system_clock::time_point, 
                            std::unordered_map<std::string, double>>> history;
        std::mutex stats_mutex;
        
        ComponentStats(const std::string& n, const std::string& t) : name(n), type(t) {}
    };

    struct Alert {
        std::string component_name;
        std::string component_type;
        std::string message;
        std::string severity; // "warning", "error", "critical"
        std::chrono::system_clock::time_point timestamp;
        bool active;
        
        Alert(const std::string& comp_name, const std::string& comp_type,
              const std::string& msg, const std::string& sev)
            : component_name(comp_name), component_type(comp_type),
              message(msg), severity(sev),
              timestamp(std::chrono::system_clock::now()), active(true) {}
    };

    MonitoringConfig config_;
    std::atomic<bool> active_{false};
    std::atomic<bool> shutdown_requested_{false};
    
    // Monitored components
    std::vector<std::weak_ptr<StreamTable>> monitored_tables_;
    std::vector<std::weak_ptr<Publisher>> monitored_publishers_;
    std::vector<std::weak_ptr<Subscriber>> monitored_subscribers_;
    std::vector<std::weak_ptr<StreamEngine>> monitored_engines_;
    mutable std::shared_mutex components_mutex_;
    
    // Statistics storage
    std::unordered_map<std::string, std::unique_ptr<ComponentStats>> component_stats_;
    mutable std::shared_mutex stats_mutex_;
    
    // Alerts
    std::vector<Alert> alerts_;
    mutable std::mutex alerts_mutex_;
    
    // Background thread
    std::unique_ptr<std::thread> monitoring_thread_;
    
    // System metrics
    mutable std::atomic<double> cpu_usage_{0.0};
    mutable std::atomic<double> memory_usage_{0.0};
    mutable std::atomic<int64_t> total_threads_{0};
    
    // Private methods
    void monitoring_worker();
    void collect_statistics();
    void collect_table_stats(std::shared_ptr<StreamTable> table);
    void collect_publisher_stats(std::shared_ptr<Publisher> publisher);
    void collect_subscriber_stats(std::shared_ptr<Subscriber> subscriber);
    void collect_engine_stats(std::shared_ptr<StreamEngine> engine);
    void collect_system_stats();
    
    void update_component_stats(const std::string& component_name,
                               const std::string& component_type,
                               const std::unordered_map<std::string, double>& stats);
    
    void check_alerts();
    void add_alert(const std::string& component_name,
                   const std::string& component_type,
                   const std::string& message,
                   const std::string& severity);
    void clear_alert(const std::string& component_name,
                     const std::string& component_type,
                     const std::string& message);
    
    void cleanup_old_history();
    double get_cpu_usage() const;
    double get_memory_usage() const;
    int64_t get_thread_count() const;
    
    std::string format_timestamp(const std::chrono::system_clock::time_point& tp) const;
    std::string format_duration(const std::chrono::system_clock::duration& duration) const;
};

/**
 * Global monitoring instance
 */
class GlobalMonitor {
public:
    static StreamingMonitor& instance() {
        static StreamingMonitor::MonitoringConfig config;
        static StreamingMonitor monitor(config);
        return monitor;
    }
    
    static void start() {
        instance().start();
    }
    
    static void stop() {
        instance().stop();
    }
    
    static auto get_streaming_stat() {
        return instance().get_streaming_stat();
    }
    
    static auto get_stream_engine_stat() {
        return instance().get_stream_engine_stat();
    }
};

} // namespace streaming_compute