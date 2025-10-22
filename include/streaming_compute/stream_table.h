#pragma once

#include "common.h"
#include <deque>

namespace streaming_compute {

/**
 * StreamTable - A special in-memory table for storing and publishing streaming data
 * 
 * Features:
 * - Thread-safe concurrent read/write operations
 * - Append-only semantics (no updates or deletes)
 * - Uses Apache Arrow RecordBatch as underlying data structure
 * - Optional persistence to disk
 * - Memory management with configurable limits
 */
class StreamTable {
public:
    /**
     * Constructor
     * @param name Table name
     * @param schema Arrow schema defining the table structure
     * @param config Configuration parameters
     */
    StreamTable(const std::string& name, SchemaPtr schema, const StreamConfig& config = StreamConfig{});
    
    /**
     * Destructor
     */
    ~StreamTable();

    /**
     * Append a RecordBatch to the table
     * @param batch RecordBatch to append
     * @return true if successful, false otherwise
     */
    virtual bool append(RecordBatchPtr batch);
    
    /**
     * Append multiple RecordBatches to the table
     * @param batches Vector of RecordBatches to append
     * @return true if successful, false otherwise
     */
    virtual bool append(const std::vector<RecordBatchPtr>& batches);

    /**
     * Get records starting from a specific offset
     * @param offset Starting offset (0-based)
     * @param limit Maximum number of records to return (-1 for all)
     * @return Vector of RecordBatches
     */
    std::vector<RecordBatchPtr> select(int64_t offset = 0, int64_t limit = -1) const;

    /**
     * Get the current size (number of rows) in the table
     * @return Number of rows
     */
    int64_t size() const;

    /**
     * Get the current offset (total number of records ever added)
     * @return Current offset
     */
    int64_t current_offset() const;

    /**
     * Get the table schema
     * @return Arrow schema
     */
    SchemaPtr schema() const { return schema_; }

    /**
     * Get the table name
     * @return Table name
     */
    const std::string& name() const { return name_; }

    /**
     * Check if the table is shared (can be published)
     * @return true if shared, false otherwise
     */
    bool is_shared() const { return shared_; }

    /**
     * Share the table (make it available for publishing)
     * @param shared_name Name to use when sharing
     */
    void share(const std::string& shared_name = "");

    /**
     * Unshare the table
     */
    void unshare();

    /**
     * Enable persistence
     * @param persistence_dir Directory for persistence files
     * @param cache_size Number of rows to keep in memory
     * @param async_write Whether to use async writes
     * @param compress Whether to compress persisted data
     */
    void enable_persistence(const std::string& persistence_dir, 
                           size_t cache_size = 1000000,
                           bool async_write = true, 
                           bool compress = true);

    /**
     * Disable persistence
     */
    void disable_persistence();

    /**
     * Clear persisted data
     */
    void clear_persistence();

    /**
     * Get persistence metadata
     * @return Dictionary with persistence information
     */
    std::unordered_map<std::string, std::string> get_persistence_meta() const;

    /**
     * Subscribe to table changes
     * @param handler Function to call when new data is added
     * @param offset Starting offset for subscription
     */
    void subscribe(MessageHandler handler, int64_t offset = -1);

    /**
     * Unsubscribe from table changes
     * @param handler Handler to remove
     */
    void unsubscribe(MessageHandler handler);

    /**
     * Get table statistics
     * @return Statistics about the table
     */
    std::unordered_map<std::string, int64_t> get_stats() const;

private:
    std::string name_;
    std::string shared_name_;
    SchemaPtr schema_;
    StreamConfig config_;
    
    mutable std::shared_mutex mutex_;
    std::deque<RecordBatchPtr> batches_;
    std::atomic<int64_t> total_rows_{0};
    std::atomic<int64_t> current_offset_{0};
    std::atomic<bool> shared_{false};
    
    // Persistence related
    std::atomic<bool> persistence_enabled_{false};
    std::string persistence_dir_;
    size_t cache_size_;
    bool async_write_;
    bool compress_;
    std::atomic<int64_t> persisted_offset_{0};
    std::unique_ptr<std::thread> persistence_thread_;
    std::atomic<bool> stop_persistence_{false};
    
    // Subscription related
    std::vector<MessageHandler> subscribers_;
    std::mutex subscribers_mutex_;
    
    // Statistics
    mutable std::atomic<int64_t> total_appends_{0};
    mutable std::atomic<int64_t> total_selects_{0};
    
    // Private methods
    void notify_subscribers(RecordBatchPtr batch);
    void persistence_worker();
    void manage_memory();
    bool persist_batch(RecordBatchPtr batch);
    std::vector<RecordBatchPtr> load_persisted_data(int64_t offset, int64_t limit) const;
};

/**
 * KeyedStreamTable - A StreamTable with primary key constraints
 * 
 * Records with duplicate keys are rejected
 */
class KeyedStreamTable : public StreamTable {
public:
    KeyedStreamTable(const std::string& name, 
                    const std::vector<std::string>& key_columns,
                    SchemaPtr schema, 
                    const StreamConfig& config = StreamConfig{});

    bool append(RecordBatchPtr batch) override;
    bool append(const std::vector<RecordBatchPtr>& batches) override;

private:
    std::vector<std::string> key_columns_;
    std::unordered_set<std::string> existing_keys_;
    std::mutex keys_mutex_;
    
    std::string compute_key(RecordBatchPtr batch, int64_t row_index) const;
};

} // namespace streaming_compute