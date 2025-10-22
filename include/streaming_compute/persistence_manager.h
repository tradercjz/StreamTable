#pragma once

#include "common.h"
#include "stream_table.h"
#include <fstream>

namespace streaming_compute {

/**
 * PersistenceManager - Manages data persistence and recovery
 * 
 * Features:
 * - Write-ahead logging (WAL)
 * - Snapshot management
 * - Data compression
 * - Recovery mechanisms
 * - Configurable retention policies
 */
class PersistenceManager {
public:
    struct PersistenceConfig {
        std::string persistence_dir = "./data";
        bool enable_compression = true;
        bool async_write = true;
        size_t wal_buffer_size = 1024 * 1024; // 1MB
        size_t snapshot_interval_seconds = 300; // 5 minutes
        size_t retention_hours = 24; // 24 hours
        size_t max_wal_size = 100 * 1024 * 1024; // 100MB
    };

    /**
     * Constructor
     * @param config Persistence configuration
     */
    explicit PersistenceManager(const PersistenceConfig& config);

    /**
     * Destructor
     */
    ~PersistenceManager();

    /**
     * Initialize persistence system
     * @return true if successful, false otherwise
     */
    bool initialize();

    /**
     * Shutdown persistence system
     */
    void shutdown();

    /**
     * Register a table for persistence
     * @param table Table to register
     * @return true if successful, false otherwise
     */
    bool register_table(std::shared_ptr<StreamTable> table);

    /**
     * Unregister a table from persistence
     * @param table_name Table name to unregister
     * @return true if successful, false otherwise
     */
    bool unregister_table(const std::string& table_name);

    /**
     * Write a record batch to WAL
     * @param table_name Table name
     * @param batch Record batch to write
     * @param offset Offset of the batch
     * @return true if successful, false otherwise
     */
    bool write_to_wal(const std::string& table_name, RecordBatchPtr batch, int64_t offset);

    /**
     * Create a snapshot for a table
     * @param table_name Table name
     * @return true if successful, false otherwise
     */
    bool create_snapshot(const std::string& table_name);

    /**
     * Recover a table from persistence
     * @param table_name Table name
     * @param target_table Target table to recover into
     * @return true if successful, false otherwise
     */
    bool recover_table(const std::string& table_name, std::shared_ptr<StreamTable> target_table);

    /**
     * Get recovery offset for a table
     * @param table_name Table name
     * @return Recovery offset, -1 if not found
     */
    int64_t get_recovery_offset(const std::string& table_name) const;

    /**
     * Clean up old persistence files
     * @return Number of files cleaned up
     */
    size_t cleanup_old_files();

    /**
     * Get persistence statistics
     * @return Map of statistics
     */
    std::unordered_map<std::string, int64_t> get_stats() const;

    /**
     * Check if persistence is active
     * @return true if active, false otherwise
     */
    bool is_active() const { return active_; }

private:
    struct TablePersistenceInfo {
        std::string table_name;
        std::shared_ptr<StreamTable> table;
        std::string wal_path;
        std::string snapshot_path;
        int64_t last_snapshot_offset = -1;
        int64_t last_wal_offset = -1;
        std::chrono::system_clock::time_point last_snapshot_time;
        std::unique_ptr<std::ofstream> wal_file;
        std::mutex wal_mutex;
        
        TablePersistenceInfo(const std::string& name, std::shared_ptr<StreamTable> tbl)
            : table_name(name), table(std::move(tbl)),
              last_snapshot_time(std::chrono::system_clock::now()) {}
    };

    PersistenceConfig config_;
    std::atomic<bool> active_{false};
    std::atomic<bool> shutdown_requested_{false};
    
    // Registered tables
    std::unordered_map<std::string, std::unique_ptr<TablePersistenceInfo>> tables_;
    mutable std::shared_mutex tables_mutex_;
    
    // Background threads
    std::unique_ptr<std::thread> snapshot_thread_;
    std::unique_ptr<std::thread> cleanup_thread_;
    
    // WAL management
    std::queue<std::function<void()>> wal_tasks_;
    std::mutex wal_tasks_mutex_;
    std::condition_variable wal_condition_;
    std::unique_ptr<std::thread> wal_thread_;
    
    // Statistics
    std::atomic<int64_t> total_wal_writes_{0};
    std::atomic<int64_t> total_snapshots_{0};
    std::atomic<int64_t> total_recoveries_{0};
    std::atomic<int64_t> bytes_written_{0};
    
    // Private methods
    void snapshot_worker();
    void cleanup_worker();
    void wal_worker();
    
    bool create_directories();
    std::string get_wal_path(const std::string& table_name) const;
    std::string get_snapshot_path(const std::string& table_name) const;
    std::string get_metadata_path(const std::string& table_name) const;
    
    bool write_wal_entry(TablePersistenceInfo& info, RecordBatchPtr batch, int64_t offset);
    bool create_table_snapshot(TablePersistenceInfo& info);
    bool recover_from_wal(const std::string& table_name, std::shared_ptr<StreamTable> target_table);
    bool recover_from_snapshot(const std::string& table_name, std::shared_ptr<StreamTable> target_table);
    
    bool save_metadata(const TablePersistenceInfo& info);
    bool load_metadata(TablePersistenceInfo& info);
    
    void rotate_wal_if_needed(TablePersistenceInfo& info);
    bool compress_file(const std::string& input_path, const std::string& output_path);
    bool decompress_file(const std::string& input_path, const std::string& output_path);
};

} // namespace streaming_compute