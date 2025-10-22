#pragma once

#include "common.h"
#include "stream_table.h"

namespace streaming_compute {

/**
 * StreamEngine - Base class for all streaming computation engines
 * 
 * Features:
 * - Message processing interface
 * - State management
 * - Output table integration
 * - Statistics collection
 * - Snapshot support for fault tolerance
 */
class StreamEngine {
public:
    /**
     * Constructor
     * @param name Engine name
     * @param dummy_table Input table schema reference
     * @param output_table Output table for results
     * @param key_column Column name for partitioning (optional)
     */
    StreamEngine(const std::string& name,
                std::shared_ptr<StreamTable> dummy_table,
                std::shared_ptr<StreamTable> output_table,
                const std::string& key_column = "");

    /**
     * Virtual destructor
     */
    virtual ~StreamEngine() = default;

    /**
     * Process a single message
     * @param message Input message to process
     * @return true if successful, false otherwise
     */
    virtual bool process_message(const StreamMessage& message) = 0;

    /**
     * Process a batch of messages
     * @param messages Vector of messages to process
     * @return Number of successfully processed messages
     */
    virtual size_t process_batch(const std::vector<StreamMessage>& messages);

    /**
     * Get engine name
     * @return Engine name
     */
    const std::string& name() const { return name_; }

    /**
     * Get key column
     * @return Key column name
     */
    const std::string& key_column() const { return key_column_; }

    /**
     * Get input schema
     * @return Input schema
     */
    SchemaPtr input_schema() const { return dummy_table_->schema(); }

    /**
     * Get output table
     * @return Output table
     */
    std::shared_ptr<StreamTable> output_table() const { return output_table_; }

    /**
     * Enable snapshot functionality
     * @param snapshot_dir Directory for snapshots
     * @param snapshot_interval_msg_count Messages between snapshots
     * @return true if successful, false otherwise
     */
    virtual bool enable_snapshot(const std::string& snapshot_dir, 
                                size_t snapshot_interval_msg_count = 20000);

    /**
     * Disable snapshot functionality
     */
    virtual void disable_snapshot();

    /**
     * Get last snapshot message ID
     * @return Message ID of last snapshot, -1 if no snapshot exists
     */
    virtual int64_t get_snapshot_msg_id() const;

    /**
     * Load state from snapshot
     * @param snapshot_path Path to snapshot file
     * @return true if successful, false otherwise
     */
    virtual bool load_snapshot(const std::string& snapshot_path);

    /**
     * Save current state to snapshot
     * @param snapshot_path Path to save snapshot
     * @return true if successful, false otherwise
     */
    virtual bool save_snapshot(const std::string& snapshot_path);

    /**
     * Get engine statistics
     * @return Map of statistics
     */
    virtual std::unordered_map<std::string, int64_t> get_stats() const;

    /**
     * Reset engine state
     */
    virtual void reset();

    /**
     * Check if engine is active
     * @return true if active, false otherwise
     */
    bool is_active() const { return active_; }

    /**
     * Set engine active state
     * @param active Active state
     */
    void set_active(bool active) { active_ = active; }

protected:
    std::string name_;
    std::shared_ptr<StreamTable> dummy_table_;
    std::shared_ptr<StreamTable> output_table_;
    std::string key_column_;
    
    // State management
    std::atomic<bool> active_{true};
    std::atomic<int64_t> processed_messages_{0};
    std::atomic<int64_t> failed_messages_{0};
    std::atomic<int64_t> last_processed_msg_id_{-1};
    
    // Snapshot support
    bool snapshot_enabled_{false};
    std::string snapshot_dir_;
    size_t snapshot_interval_msg_count_;
    std::atomic<int64_t> last_snapshot_msg_id_{-1};
    mutable std::mutex snapshot_mutex_;
    
    // Thread safety
    mutable std::shared_mutex state_mutex_;
    
    // Helper methods
    virtual void update_statistics(bool success);
    virtual bool should_create_snapshot() const;
    virtual void create_snapshot_if_needed();
    virtual std::string get_snapshot_path() const;
    
    // Serialization methods (to be implemented by derived classes)
    virtual bool serialize_state(std::ostream& out) const;
    virtual bool deserialize_state(std::istream& in);
};

/**
 * Engine factory for creating different types of engines
 */
class StreamEngineFactory {
public:
    /**
     * Create a time series engine
     */
    static std::shared_ptr<StreamEngine> create_time_series_engine(
        const std::string& name,
        const std::string& metrics,
        std::shared_ptr<StreamTable> dummy_table,
        std::shared_ptr<StreamTable> output_table,
        const std::string& key_column = "",
        size_t window_size = 100);

    /**
     * Create a reactive state engine
     */
    static std::shared_ptr<StreamEngine> create_reactive_state_engine(
        const std::string& name,
        const std::string& metrics,
        std::shared_ptr<StreamTable> dummy_table,
        std::shared_ptr<StreamTable> output_table,
        const std::string& key_column = "");

    /**
     * Create a cross-sectional engine
     */
    static std::shared_ptr<StreamEngine> create_cross_sectional_engine(
        const std::string& name,
        const std::string& metrics,
        std::shared_ptr<StreamTable> dummy_table,
        std::shared_ptr<StreamTable> output_table,
        const std::string& key_column = "",
        const std::string& triggering_pattern = "keyCount",
        size_t triggering_interval = 1000);

    /**
     * Create an anomaly detection engine
     */
    static std::shared_ptr<StreamEngine> create_anomaly_detection_engine(
        const std::string& name,
        const std::string& metrics,
        std::shared_ptr<StreamTable> dummy_table,
        std::shared_ptr<StreamTable> output_table,
        const std::string& key_column = "",
        double threshold = 2.0);
};

} // namespace streaming_compute