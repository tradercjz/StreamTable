#pragma once

/**
 * Streaming Compute Library
 * 
 * A comprehensive C++ streaming computation system based on Apache Arrow RecordBatch,
 * inspired by DolphinDB's streaming framework.
 * 
 * Features:
 * - High-throughput, low-latency streaming data processing
 * - Apache Arrow RecordBatch-based data structures
 * - Publisher-Subscriber pattern for data distribution
 * - Multiple streaming computation engines
 * - Data persistence and recovery mechanisms
 * - Comprehensive monitoring and statistics
 * - Thread-safe operations
 */

// Core components
#include "common.h"
#include "stream_table.h"
#include "publisher.h"
#include "subscriber.h"

// Streaming engines
#include "stream_engine.h"
#include "time_series_engine.h"
#include "reactive_state_engine.h"
#include "cross_sectional_engine.h"
#include "cep_engine.h"

// Infrastructure
#include "persistence_manager.h"
#include "monitoring.h"
#include <any>

namespace streaming_compute {

/**
 * StreamingSystem - Main system class that orchestrates all components
 */
class StreamingSystem {
public:
    /**
     * Constructor
     * @param config System configuration
     */
    explicit StreamingSystem(const StreamConfig& config);

    /**
     * Destructor
     */
    ~StreamingSystem();

    /**
     * Initialize the streaming system
     * @return true if successful, false otherwise
     */
    bool initialize();

    /**
     * Shutdown the streaming system
     */
    void shutdown();

    /**
     * Create a stream table
     * @param name Table name
     * @param schema Table schema
     * @param keyed Whether to create a keyed table
     * @param key_columns Key columns (for keyed tables)
     * @return Shared pointer to the created table
     */
    std::shared_ptr<StreamTable> create_stream_table(
        const std::string& name,
        SchemaPtr schema,
        bool keyed = false,
        const std::vector<std::string>& key_columns = {});

    /**
     * Share a stream table (make it available for publishing)
     * @param table Table to share
     * @param shared_name Name to use when sharing (optional)
     * @return true if successful, false otherwise
     */
    bool share_stream_table(std::shared_ptr<StreamTable> table, 
                           const std::string& shared_name = "");

    /**
     * Drop a stream table
     * @param table_name Table name to drop
     * @return true if successful, false otherwise
     */
    bool drop_stream_table(const std::string& table_name);

    /**
     * Subscribe to a stream table
     * @param server Server address (empty for local)
     * @param table_name Table name
     * @param action_name Action name
     * @param offset Starting offset
     * @param handler Message handler
     * @param msg_as_table Whether to deliver messages as tables
     * @param batch_size Batch size
     * @param throttle Throttle time
     * @param reconnect Whether to reconnect automatically
     * @return Topic string if successful, empty otherwise
     */
    std::string subscribe_table(
        const std::string& server,
        const std::string& table_name,
        const std::string& action_name,
        int64_t offset,
        MessageHandler handler,
        bool msg_as_table = false,
        size_t batch_size = 0,
        double throttle = 1.0,
        bool reconnect = false);

    /**
     * Unsubscribe from a stream table
     * @param server Server address
     * @param table_name Table name
     * @param action_name Action name
     * @return true if successful, false otherwise
     */
    bool unsubscribe_table(const std::string& server,
                          const std::string& table_name,
                          const std::string& action_name);

    /**
     * Create a time series engine
     * @param name Engine name
     * @param metrics Metrics to compute
     * @param dummy_table Input table schema reference
     * @param output_table Output table
     * @param key_column Key column
     * @param window_size Window size
     * @return Shared pointer to the created engine
     */
    std::shared_ptr<StreamEngine> create_time_series_engine(
        const std::string& name,
        const std::string& metrics,
        std::shared_ptr<StreamTable> dummy_table,
        std::shared_ptr<StreamTable> output_table,
        const std::string& key_column = "",
        size_t window_size = 100);

    /**
     * Create a reactive state engine
     * @param name Engine name
     * @param metrics Metrics to compute
     * @param dummy_table Input table schema reference
     * @param output_table Output table
     * @param key_column Key column
     * @return Shared pointer to the created engine
     */
    std::shared_ptr<StreamEngine> create_reactive_state_engine(
        const std::string& name,
        const std::string& metrics,
        std::shared_ptr<StreamTable> dummy_table,
        std::shared_ptr<StreamTable> output_table,
        const std::string& key_column = "");

    /**
     * Create a cross-sectional engine
     * @param name Engine name
     * @param metrics Metrics to compute
     * @param dummy_table Input table schema reference
     * @param output_table Output table
     * @param key_column Key column
     * @param triggering_pattern Triggering pattern
     * @param triggering_interval Triggering interval
     * @return Shared pointer to the created engine
     */
    std::shared_ptr<StreamEngine> create_cross_sectional_engine(
        const std::string& name,
        const std::string& metrics,
        std::shared_ptr<StreamTable> dummy_table,
        std::shared_ptr<StreamTable> output_table,
        const std::string& key_column = "",
        const std::string& triggering_pattern = "keyCount",
        size_t triggering_interval = 1000);

    /**
     * Create a CEP (Complex Event Processing) engine
     * @param name Engine name
     * @param monitors Monitor classes for event handling
     * @param dummy_table Input table schema reference
     * @param event_schemas Event schemas definition
     * @param output_table Output table
     * @param dispatch_key Key for sub-engine distribution
     * @param dispatch_bucket Number of sub-engines
     * @param time_column Time column name
     * @param event_time_field Event time field name
     * @param use_system_time Whether to use system time
     * @param event_queue_depth Event queue depth
     * @param deserialize_parallelism Deserialization parallelism
     * @return Shared pointer to the created engine
     */
    std::shared_ptr<StreamEngine> create_cep_engine(
        const std::string& name,
        const std::vector<std::shared_ptr<CEPMonitor>>& monitors,
        std::shared_ptr<StreamTable> dummy_table,
        const std::vector<EventSchema>& event_schemas,
        std::shared_ptr<StreamTable> output_table = nullptr,
        const std::string& dispatch_key = "",
        size_t dispatch_bucket = 1,
        const std::string& time_column = "",
        const std::string& event_time_field = "eventTime",
        bool use_system_time = true,
        size_t event_queue_depth = 1024,
        size_t deserialize_parallelism = 1);

    /**
     * Drop a stream engine
     * @param name Engine name
     * @return true if successful, false otherwise
     */
    bool drop_stream_engine(const std::string& name);

    /**
     * Get a stream engine by name
     * @param name Engine name
     * @return Shared pointer to the engine, nullptr if not found
     */
    std::shared_ptr<StreamEngine> get_stream_engine(const std::string& name);

    /**
     * Enable table persistence
     * @param table Table to enable persistence for
     * @param cache_size Cache size
     * @param async_write Whether to use async writes
     * @param compress Whether to compress data
     * @return true if successful, false otherwise
     */
    bool enable_table_persistence(std::shared_ptr<StreamTable> table,
                                 size_t cache_size = 1000000,
                                 bool async_write = true,
                                 bool compress = true);

    /**
     * Get streaming statistics (equivalent to DolphinDB's getStreamingStat)
     * @return Dictionary containing all streaming statistics
     */
    auto get_streaming_stat() const {
        return monitor_->get_streaming_stat();
    }

    /**
     * Get stream engine statistics
     * @return Dictionary containing engine statistics
     */
    auto get_stream_engine_stat() const {
        return monitor_->get_stream_engine_stat();
    }

    /**
     * Get system performance metrics
     * @return Map of performance metrics
     */
    std::unordered_map<std::string, double> get_performance_metrics() const {
        return monitor_->get_performance_metrics();
    }

    /**
     * Check if system is running
     * @return true if running, false otherwise
     */
    bool is_running() const { return running_; }

private:
    StreamConfig config_;
    std::atomic<bool> running_{false};
    
    // Core components
    std::unique_ptr<Publisher> publisher_;
    std::unique_ptr<Subscriber> subscriber_;
    std::unique_ptr<PersistenceManager> persistence_manager_;
    std::unique_ptr<StreamingMonitor> monitor_;
    
    // Managed objects
    std::unordered_map<std::string, std::shared_ptr<StreamTable>> tables_;
    std::unordered_map<std::string, std::shared_ptr<StreamEngine>> engines_;
    std::shared_mutex objects_mutex_;
    
    // Helper methods
    void register_components_for_monitoring();
};

// Convenience functions for quick setup

/**
 * Create a simple streaming table with default configuration
 */
inline std::shared_ptr<StreamTable> streamTable(
    size_t initial_capacity,
    const std::vector<std::string>& column_names,
    const std::vector<std::shared_ptr<arrow::DataType>>& column_types) {
    
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (size_t i = 0; i < column_names.size() && i < column_types.size(); ++i) {
        fields.push_back(arrow::field(column_names[i], column_types[i]));
    }
    
    auto schema = arrow::schema(fields);
    return std::make_shared<StreamTable>("temp_table", schema);
}

/**
 * Create a keyed streaming table with default configuration
 */
inline std::shared_ptr<StreamTable> keyedStreamTable(
    const std::vector<std::string>& key_columns,
    size_t initial_capacity,
    const std::vector<std::string>& column_names,
    const std::vector<std::shared_ptr<arrow::DataType>>& column_types) {
    
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (size_t i = 0; i < column_names.size() && i < column_types.size(); ++i) {
        fields.push_back(arrow::field(column_names[i], column_types[i]));
    }
    
    auto schema = arrow::schema(fields);
    return std::make_shared<KeyedStreamTable>("temp_keyed_table", key_columns, schema);
}

} // namespace streaming_compute
