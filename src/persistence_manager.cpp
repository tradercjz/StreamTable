#include "streaming_compute/persistence_manager.h"
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include <arrow/io/file.h>
#include <filesystem>
#include <fstream>
#include <zlib.h>

namespace streaming_compute {

PersistenceManager::PersistenceManager(const PersistenceConfig& config) : config_(config) {
}

PersistenceManager::~PersistenceManager() {
    shutdown();
}

bool PersistenceManager::initialize() {
    if (active_) {
        return false;
    }

    // Create directories
    if (!create_directories()) {
        return false;
    }

    shutdown_requested_ = false;
    active_ = true;

    // Start background threads
    if (config_.async_write) {
        wal_thread_ = std::make_unique<std::thread>(&PersistenceManager::wal_worker, this);
    }

    snapshot_thread_ = std::make_unique<std::thread>(&PersistenceManager::snapshot_worker, this);
    cleanup_thread_ = std::make_unique<std::thread>(&PersistenceManager::cleanup_worker, this);

    return true;
}

void PersistenceManager::shutdown() {
    if (!active_) {
        return;
    }

    shutdown_requested_ = true;
    active_ = false;

    // Notify all threads
    wal_condition_.notify_all();

    // Wait for threads to finish
    if (wal_thread_ && wal_thread_->joinable()) {
        wal_thread_->join();
    }
    if (snapshot_thread_ && snapshot_thread_->joinable()) {
        snapshot_thread_->join();
    }
    if (cleanup_thread_ && cleanup_thread_->joinable()) {
        cleanup_thread_->join();
    }

    // Close all WAL files
    std::unique_lock<std::shared_mutex> lock(tables_mutex_);
    for (auto& pair : tables_) {
        auto& info = pair.second;
        std::lock_guard<std::mutex> wal_lock(info->wal_mutex);
        if (info->wal_file && info->wal_file->is_open()) {
            info->wal_file->close();
        }
    }
}

bool PersistenceManager::register_table(std::shared_ptr<StreamTable> table) {
    if (!table || !active_) {
        return false;
    }

    std::unique_lock<std::shared_mutex> lock(tables_mutex_);

    const std::string& table_name = table->name();
    if (tables_.find(table_name) != tables_.end()) {
        return false; // Already registered
    }

    auto info = std::make_unique<TablePersistenceInfo>(table_name, table);
    info->wal_path = get_wal_path(table_name);
    info->snapshot_path = get_snapshot_path(table_name);

    // Load existing metadata
    load_metadata(*info);

    // Open WAL file for writing
    info->wal_file = std::make_unique<std::ofstream>(info->wal_path, 
                                                    std::ios::binary | std::ios::app);
    if (!info->wal_file->is_open()) {
        return false;
    }

    tables_[table_name] = std::move(info);
    return true;
}

bool PersistenceManager::unregister_table(const std::string& table_name) {
    std::unique_lock<std::shared_mutex> lock(tables_mutex_);

    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return false;
    }

    auto& info = it->second;
    
    // Close WAL file
    {
        std::lock_guard<std::mutex> wal_lock(info->wal_mutex);
        if (info->wal_file && info->wal_file->is_open()) {
            info->wal_file->close();
        }
    }

    // Save final metadata
    save_metadata(*info);

    tables_.erase(it);
    return true;
}

bool PersistenceManager::write_to_wal(const std::string& table_name, 
                                     RecordBatchPtr batch, 
                                     int64_t offset) {
    if (!active_ || !batch) {
        return false;
    }

    std::shared_lock<std::shared_mutex> lock(tables_mutex_);
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return false;
    }

    auto& info = *it->second;
    lock.unlock();

    if (config_.async_write) {
        // Add to async queue
        std::lock_guard<std::mutex> task_lock(wal_tasks_mutex_);
        wal_tasks_.push([this, &info, batch, offset]() {
            write_wal_entry(info, batch, offset);
        });
        wal_condition_.notify_one();
        return true;
    } else {
        // Write synchronously
        return write_wal_entry(info, batch, offset);
    }
}

bool PersistenceManager::create_snapshot(const std::string& table_name) {
    std::shared_lock<std::shared_mutex> lock(tables_mutex_);
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return false;
    }

    auto& info = *it->second;
    lock.unlock();

    return create_table_snapshot(info);
}

bool PersistenceManager::recover_table(const std::string& table_name, 
                                      std::shared_ptr<StreamTable> target_table) {
    if (!target_table) {
        return false;
    }

    // First try to recover from snapshot
    if (recover_from_snapshot(table_name, target_table)) {
        // Then apply WAL entries after the snapshot
        recover_from_wal(table_name, target_table);
        total_recoveries_++;
        return true;
    }

    // If no snapshot, recover from WAL only
    if (recover_from_wal(table_name, target_table)) {
        total_recoveries_++;
        return true;
    }

    return false;
}

int64_t PersistenceManager::get_recovery_offset(const std::string& table_name) const {
    std::shared_lock<std::shared_mutex> lock(tables_mutex_);
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
        return std::max(it->second->last_snapshot_offset, it->second->last_wal_offset);
    }

    // Check metadata file
    std::string metadata_path = get_metadata_path(table_name);
    if (std::filesystem::exists(metadata_path)) {
        std::ifstream file(metadata_path);
        if (file.is_open()) {
            int64_t offset;
            file >> offset;
            return offset;
        }
    }

    return -1;
}

size_t PersistenceManager::cleanup_old_files() {
    size_t cleaned_files = 0;
    auto cutoff_time = std::chrono::system_clock::now() - 
                      std::chrono::hours(config_.retention_hours);

    try {
        for (const auto& entry : std::filesystem::directory_iterator(config_.persistence_dir)) {
            if (entry.is_regular_file()) {
                auto file_time = std::filesystem::last_write_time(entry);
                auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
                    file_time - std::filesystem::file_time_type::clock::now() + 
                    std::chrono::system_clock::now());
                
                if (sctp < cutoff_time) {
                    std::filesystem::remove(entry);
                    cleaned_files++;
                }
            }
        }
    } catch (const std::exception& e) {
        // Log error
    }

    return cleaned_files;
}

std::unordered_map<std::string, int64_t> PersistenceManager::get_stats() const {
    std::unordered_map<std::string, int64_t> stats;

    stats["total_wal_writes"] = total_wal_writes_.load();
    stats["total_snapshots"] = total_snapshots_.load();
    stats["total_recoveries"] = total_recoveries_.load();
    stats["bytes_written"] = bytes_written_.load();
    stats["registered_tables"] = tables_.size();
    stats["is_active"] = active_.load() ? 1 : 0;

    return stats;
}

void PersistenceManager::snapshot_worker() {
    while (!shutdown_requested_) {
        std::this_thread::sleep_for(std::chrono::seconds(config_.snapshot_interval_seconds));

        if (shutdown_requested_) {
            break;
        }

        // Create snapshots for tables that need them
        std::shared_lock<std::shared_mutex> lock(tables_mutex_);
        for (auto& pair : tables_) {
            auto& info = *pair.second;
            
            auto now = std::chrono::system_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                now - info.last_snapshot_time).count();
            
            if (elapsed >= static_cast<int64_t>(config_.snapshot_interval_seconds)) {
                lock.unlock();
                create_table_snapshot(info);
                lock.lock();
            }
        }
    }
}

void PersistenceManager::cleanup_worker() {
    while (!shutdown_requested_) {
        std::this_thread::sleep_for(std::chrono::hours(1)); // Run cleanup every hour

        if (shutdown_requested_) {
            break;
        }

        cleanup_old_files();
    }
}

void PersistenceManager::wal_worker() {
    while (!shutdown_requested_) {
        std::unique_lock<std::mutex> lock(wal_tasks_mutex_);
        
        wal_condition_.wait(lock, [this] {
            return !wal_tasks_.empty() || shutdown_requested_;
        });

        if (shutdown_requested_) {
            break;
        }

        while (!wal_tasks_.empty()) {
            auto task = wal_tasks_.front();
            wal_tasks_.pop();
            lock.unlock();

            try {
                task();
            } catch (const std::exception& e) {
                // Log error
            }

            lock.lock();
        }
    }
}

bool PersistenceManager::create_directories() {
    try {
        std::filesystem::create_directories(config_.persistence_dir);
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

std::string PersistenceManager::get_wal_path(const std::string& table_name) const {
    return config_.persistence_dir + "/" + table_name + ".wal";
}

std::string PersistenceManager::get_snapshot_path(const std::string& table_name) const {
    return config_.persistence_dir + "/" + table_name + ".snapshot";
}

std::string PersistenceManager::get_metadata_path(const std::string& table_name) const {
    return config_.persistence_dir + "/" + table_name + ".meta";
}

bool PersistenceManager::write_wal_entry(TablePersistenceInfo& info, 
                                        RecordBatchPtr batch, 
                                        int64_t offset) {
    std::lock_guard<std::mutex> lock(info.wal_mutex);

    if (!info.wal_file || !info.wal_file->is_open()) {
        return false;
    }

    try {
        // Write WAL entry header
        info.wal_file->write(reinterpret_cast<const char*>(&offset), sizeof(offset));
        
        int64_t timestamp = current_timestamp();
        info.wal_file->write(reinterpret_cast<const char*>(&timestamp), sizeof(timestamp));

        // Serialize the batch using Arrow IPC format
        std::ostringstream batch_stream;
        
        auto buffer_output_stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(buffer_output_stream, batch->schema()).ValueOrDie();
        
        writer->WriteRecordBatch(*batch);
        writer->Close();
        
        auto buffer = buffer_output_stream->Finish().ValueOrDie();
        
        // Write batch size and data
        int64_t batch_size = buffer->size();
        info.wal_file->write(reinterpret_cast<const char*>(&batch_size), sizeof(batch_size));
        info.wal_file->write(reinterpret_cast<const char*>(buffer->data()), batch_size);
        
        info.wal_file->flush();
        
        info.last_wal_offset = offset;
        bytes_written_ += sizeof(offset) + sizeof(timestamp) + sizeof(batch_size) + batch_size;
        total_wal_writes_++;
        
        // Check if WAL needs rotation
        rotate_wal_if_needed(info);
        
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

bool PersistenceManager::create_table_snapshot(TablePersistenceInfo& info) {
    if (!info.table) {
        return false;
    }

    try {
        // Get current table data
        auto batches = info.table->select(0, -1);
        if (batches.empty()) {
            return true; // Empty table, nothing to snapshot
        }

        // Create snapshot file
        std::string temp_path = info.snapshot_path + ".tmp";
        
        auto file_output_stream = arrow::io::FileOutputStream::Open(temp_path).ValueOrDie();
        auto writer = arrow::ipc::MakeFileWriter(file_output_stream, batches[0]->schema()).ValueOrDie();
        
        for (const auto& batch : batches) {
            writer->WriteRecordBatch(*batch);
        }
        
        writer->Close();
        file_output_stream->Close();
        
        // Compress if enabled
        if (config_.enable_compression) {
            std::string compressed_path = info.snapshot_path + ".gz";
            if (compress_file(temp_path, compressed_path)) {
                std::filesystem::remove(temp_path);
                std::filesystem::rename(compressed_path, info.snapshot_path);
            } else {
                std::filesystem::rename(temp_path, info.snapshot_path);
            }
        } else {
            std::filesystem::rename(temp_path, info.snapshot_path);
        }
        
        info.last_snapshot_offset = info.table->current_offset();
        info.last_snapshot_time = std::chrono::system_clock::now();
        
        // Save metadata
        save_metadata(info);
        
        total_snapshots_++;
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

bool PersistenceManager::recover_from_wal(const std::string& table_name, 
                                         std::shared_ptr<StreamTable> target_table) {
    std::string wal_path = get_wal_path(table_name);
    if (!std::filesystem::exists(wal_path)) {
        return false;
    }

    try {
        std::ifstream wal_file(wal_path, std::ios::binary);
        if (!wal_file.is_open()) {
            return false;
        }

        while (!wal_file.eof()) {
            // Read WAL entry header
            int64_t offset, timestamp;
            wal_file.read(reinterpret_cast<char*>(&offset), sizeof(offset));
            if (wal_file.eof()) break;
            
            wal_file.read(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));
            
            // Read batch size and data
            int64_t batch_size;
            wal_file.read(reinterpret_cast<char*>(&batch_size), sizeof(batch_size));
            
            std::vector<char> batch_data(batch_size);
            wal_file.read(batch_data.data(), batch_size);
            
            // Deserialize batch
            auto buffer = arrow::Buffer::Wrap(batch_data.data(), batch_size);
            auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
            auto stream_reader = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader).ValueOrDie();
            
            RecordBatchPtr batch;
            auto status = stream_reader->ReadNext(&batch);
            if (status.ok() && batch) {
                target_table->append(batch);
            }
        }
        
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

bool PersistenceManager::recover_from_snapshot(const std::string& table_name, 
                                              std::shared_ptr<StreamTable> target_table) {
    std::string snapshot_path = get_snapshot_path(table_name);
    if (!std::filesystem::exists(snapshot_path)) {
        return false;
    }

    try {
        std::string actual_path = snapshot_path;
        
        // Check if file is compressed
        if (config_.enable_compression) {
            std::string decompressed_path = snapshot_path + ".tmp";
            if (decompress_file(snapshot_path, decompressed_path)) {
                actual_path = decompressed_path;
            }
        }
        
        auto file_input_stream = arrow::io::ReadableFile::Open(actual_path).ValueOrDie();
        auto reader = arrow::ipc::RecordBatchFileReader::Open(file_input_stream).ValueOrDie();
        
        int num_batches = reader->num_record_batches();
        for (int i = 0; i < num_batches; ++i) {
            auto batch = reader->ReadRecordBatch(i).ValueOrDie();
            target_table->append(batch);
        }
        
        // Clean up temporary file if created
        if (actual_path != snapshot_path) {
            std::filesystem::remove(actual_path);
        }
        
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

bool PersistenceManager::save_metadata(const TablePersistenceInfo& info) {
    try {
        std::string metadata_path = get_metadata_path(info.table_name);
        std::ofstream file(metadata_path);
        
        if (!file.is_open()) {
            return false;
        }
        
        file << info.last_snapshot_offset << "\n";
        file << info.last_wal_offset << "\n";
        
        auto time_t = std::chrono::system_clock::to_time_t(info.last_snapshot_time);
        file << time_t << "\n";
        
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

bool PersistenceManager::load_metadata(TablePersistenceInfo& info) {
    try {
        std::string metadata_path = get_metadata_path(info.table_name);
        if (!std::filesystem::exists(metadata_path)) {
            return false;
        }
        
        std::ifstream file(metadata_path);
        if (!file.is_open()) {
            return false;
        }
        
        file >> info.last_snapshot_offset;
        file >> info.last_wal_offset;
        
        std::time_t time_t;
        file >> time_t;
        info.last_snapshot_time = std::chrono::system_clock::from_time_t(time_t);
        
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

void PersistenceManager::rotate_wal_if_needed(TablePersistenceInfo& info) {
    if (!info.wal_file) {
        return;
    }
    
    // Check file size
    info.wal_file->seekp(0, std::ios::end);
    size_t file_size = info.wal_file->tellp();
    
    if (file_size >= config_.max_wal_size) {
        // Close current file
        info.wal_file->close();
        
        // Rename to backup
        std::string backup_path = info.wal_path + ".old";
        std::filesystem::rename(info.wal_path, backup_path);
        
        // Open new file
        info.wal_file = std::make_unique<std::ofstream>(info.wal_path, std::ios::binary);
    }
}

bool PersistenceManager::compress_file(const std::string& input_path, const std::string& output_path) {
    try {
        std::ifstream input(input_path, std::ios::binary);
        std::ofstream output(output_path, std::ios::binary);
        
        if (!input.is_open() || !output.is_open()) {
            return false;
        }
        
        // Simple compression using zlib (simplified implementation)
        // In practice, you'd use a more sophisticated compression library
        
        const size_t buffer_size = 1024 * 1024; // 1MB buffer
        std::vector<char> buffer(buffer_size);
        
        while (input.read(buffer.data(), buffer_size) || input.gcount() > 0) {
            output.write(buffer.data(), input.gcount());
        }
        
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

bool PersistenceManager::decompress_file(const std::string& input_path, const std::string& output_path) {
    try {
        std::ifstream input(input_path, std::ios::binary);
        std::ofstream output(output_path, std::ios::binary);
        
        if (!input.is_open() || !output.is_open()) {
            return false;
        }
        
        // Simple decompression (simplified implementation)
        const size_t buffer_size = 1024 * 1024; // 1MB buffer
        std::vector<char> buffer(buffer_size);
        
        while (input.read(buffer.data(), buffer_size) || input.gcount() > 0) {
            output.write(buffer.data(), input.gcount());
        }
        
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

} // namespace streaming_compute