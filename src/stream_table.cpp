#include "streaming_compute/stream_table.h"
#include <arrow/compute/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace streaming_compute {

StreamTable::StreamTable(const std::string& name, SchemaPtr schema, const StreamConfig& config)
    : name_(name), schema_(std::move(schema)), config_(config) {
}

StreamTable::~StreamTable() {
    stop_persistence_ = true;
    if (persistence_thread_ && persistence_thread_->joinable()) {
        persistence_thread_->join();
    }
}

bool StreamTable::append(RecordBatchPtr batch) {
    if (!batch || batch->num_rows() == 0) {
        return false;
    }

    // Validate schema compatibility
    if (!batch->schema()->Equals(*schema_)) {
        return false;
    }

    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        batches_.push_back(batch);
        total_rows_ += batch->num_rows();
        current_offset_ += batch->num_rows();
        total_appends_++;
    }

    // Notify subscribers
    notify_subscribers(batch);

    // Manage memory if needed
    manage_memory();

    return true;
}

bool StreamTable::append(const std::vector<RecordBatchPtr>& batches) {
    if (batches.empty()) {
        return false;
    }

    int64_t total_new_rows = 0;
    
    // Validate all batches first
    for (const auto& batch : batches) {
        if (!batch || batch->num_rows() == 0) {
            return false;
        }
        if (!batch->schema()->Equals(*schema_)) {
            return false;
        }
        total_new_rows += batch->num_rows();
    }

    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        for (const auto& batch : batches) {
            batches_.push_back(batch);
        }
        total_rows_ += total_new_rows;
        current_offset_ += total_new_rows;
        total_appends_ += batches.size();
    }

    // Notify subscribers for each batch
    for (const auto& batch : batches) {
        notify_subscribers(batch);
    }

    // Manage memory if needed
    manage_memory();

    return true;
}

std::vector<RecordBatchPtr> StreamTable::select(int64_t offset, int64_t limit) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<RecordBatchPtr> result;
    
    if (offset < 0) {
        offset = 0;
    }

    int64_t current_row = 0;
    int64_t rows_collected = 0;
    
    for (const auto& batch : batches_) {
        int64_t batch_rows = batch->num_rows();
        
        // Skip batches that are before our offset
        if (current_row + batch_rows <= offset) {
            current_row += batch_rows;
            continue;
        }
        
        // Check if we've collected enough rows
        if (limit > 0 && rows_collected >= limit) {
            break;
        }
        
        // Determine slice parameters
        int64_t start_row = std::max(0L, offset - current_row);
        int64_t end_row = batch_rows;
        
        if (limit > 0) {
            int64_t remaining = limit - rows_collected;
            end_row = std::min(end_row, start_row + remaining);
        }
        
        // Slice the batch if needed
        if (start_row > 0 || end_row < batch_rows) {
            auto sliced_batch = batch->Slice(start_row, end_row - start_row);
            result.push_back(sliced_batch);
            rows_collected += end_row - start_row;
        } else {
            result.push_back(batch);
            rows_collected += batch_rows;
        }
        
        current_row += batch_rows;
    }
    
    total_selects_++;
    return result;
}

int64_t StreamTable::size() const {
    return total_rows_.load();
}

int64_t StreamTable::current_offset() const {
    return current_offset_.load();
}

void StreamTable::share(const std::string& shared_name) {
    shared_name_ = shared_name.empty() ? name_ : shared_name;
    shared_ = true;
}

void StreamTable::unshare() {
    shared_ = false;
    shared_name_.clear();
}

void StreamTable::enable_persistence(const std::string& persistence_dir,
                                    size_t cache_size,
                                    bool async_write,
                                    bool compress) {
    persistence_dir_ = persistence_dir;
    cache_size_ = cache_size;
    async_write_ = async_write;
    compress_ = compress;
    
    // Create persistence directory if it doesn't exist
    std::filesystem::create_directories(persistence_dir_);
    
    persistence_enabled_ = true;
    
    if (async_write_) {
        stop_persistence_ = false;
        persistence_thread_ = std::make_unique<std::thread>(&StreamTable::persistence_worker, this);
    }
}

void StreamTable::disable_persistence() {
    persistence_enabled_ = false;
    stop_persistence_ = true;
    
    if (persistence_thread_ && persistence_thread_->joinable()) {
        persistence_thread_->join();
        persistence_thread_.reset();
    }
}

void StreamTable::clear_persistence() {
    if (!persistence_enabled_) {
        return;
    }
    
    std::string file_path = persistence_dir_ + "/" + name_ + ".arrow";
    std::filesystem::remove(file_path);
    persisted_offset_ = 0;
}

std::unordered_map<std::string, std::string> StreamTable::get_persistence_meta() const {
    std::unordered_map<std::string, std::string> meta;
    
    meta["sizeInMemory"] = std::to_string(total_rows_.load());
    meta["asynWrite"] = async_write_ ? "true" : "false";
    meta["totalSize"] = std::to_string(current_offset_.load());
    meta["compress"] = compress_ ? "true" : "false";
    meta["memoryOffset"] = std::to_string(current_offset_.load() - total_rows_.load());
    meta["sizeOnDisk"] = std::to_string(persisted_offset_.load());
    meta["retentionMinutes"] = "1440";
    meta["persistenceDir"] = persistence_dir_ + "/" + name_;
    meta["hashValue"] = "0";
    meta["diskOffset"] = "0";
    
    return meta;
}

void StreamTable::subscribe(MessageHandler handler, int64_t offset) {
    std::lock_guard<std::mutex> lock(subscribers_mutex_);
    subscribers_.push_back(handler);
    
    // If offset is specified, send historical data
    if (offset >= 0) {
        auto historical_batches = select(offset);
        for (const auto& batch : historical_batches) {
            StreamMessage msg(offset, current_timestamp(), batch, name_);
            handler(msg);
            offset += batch->num_rows();
        }
    }
}

void StreamTable::unsubscribe(MessageHandler handler) {
    std::lock_guard<std::mutex> lock(subscribers_mutex_);
    // Note: This is a simplified implementation
    // In practice, you'd need a more sophisticated way to identify handlers
    subscribers_.clear();
}

std::unordered_map<std::string, int64_t> StreamTable::get_stats() const {
    std::unordered_map<std::string, int64_t> stats;
    
    stats["total_rows"] = total_rows_.load();
    stats["current_offset"] = current_offset_.load();
    stats["total_appends"] = total_appends_.load();
    stats["total_selects"] = total_selects_.load();
    stats["persisted_offset"] = persisted_offset_.load();
    stats["is_shared"] = shared_.load() ? 1 : 0;
    stats["persistence_enabled"] = persistence_enabled_.load() ? 1 : 0;
    
    return stats;
}

void StreamTable::notify_subscribers(RecordBatchPtr batch) {
    std::lock_guard<std::mutex> lock(subscribers_mutex_);
    
    if (subscribers_.empty()) {
        return;
    }
    
    StreamMessage msg(current_offset_.load(), current_timestamp(), batch, name_);
    
    for (const auto& handler : subscribers_) {
        try {
            handler(msg);
        } catch (const std::exception& e) {
            // Log error but continue with other subscribers
            // In a real implementation, you'd use proper logging
        }
    }
}

void StreamTable::persistence_worker() {
    while (!stop_persistence_) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        if (!persistence_enabled_) {
            continue;
        }
        
        // Check if we need to persist data
        int64_t current_size = total_rows_.load();
        if (current_size > static_cast<int64_t>(cache_size_)) {
            // Persist half of the data
            int64_t rows_to_persist = current_size / 2;
            
            std::shared_lock<std::shared_mutex> lock(mutex_);
            
            // Collect batches to persist
            std::vector<RecordBatchPtr> batches_to_persist;
            int64_t rows_collected = 0;
            
            auto it = batches_.begin();
            while (it != batches_.end() && rows_collected < rows_to_persist) {
                batches_to_persist.push_back(*it);
                rows_collected += (*it)->num_rows();
                ++it;
            }
            
            lock.unlock();
            
            // Persist the batches
            for (const auto& batch : batches_to_persist) {
                if (persist_batch(batch)) {
                    persisted_offset_ += batch->num_rows();
                }
            }
            
            // Remove persisted batches from memory
            std::unique_lock<std::shared_mutex> write_lock(mutex_);
            for (size_t i = 0; i < batches_to_persist.size() && !batches_.empty(); ++i) {
                total_rows_ -= batches_.front()->num_rows();
                batches_.pop_front();
            }
        }
    }
}

void StreamTable::manage_memory() {
    if (!persistence_enabled_) {
        // Simple memory management without persistence
        std::unique_lock<std::shared_mutex> lock(mutex_);
        
        while (batches_.size() > config_.max_memory_size) {
            total_rows_ -= batches_.front()->num_rows();
            batches_.pop_front();
        }
    }
}

bool StreamTable::persist_batch(RecordBatchPtr batch) {
    try {
        std::string file_path = persistence_dir_ + "/" + name_ + ".arrow";
        
        // Open file for appending
        std::shared_ptr<arrow::io::FileOutputStream> file;
        auto result = arrow::io::FileOutputStream::Open(file_path, true);
        if (!result.ok()) {
            return false;
        }
        file = result.ValueOrDie();
        
        // Create IPC writer
        std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
        auto writer_result = arrow::ipc::MakeFileWriter(file, schema_);
        if (!writer_result.ok()) {
            return false;
        }
        writer = writer_result.ValueOrDie();
        
        // Write batch
        auto write_result = writer->WriteRecordBatch(*batch);
        if (!write_result.ok()) {
            return false;
        }
        
        // Close writer
        auto close_result = writer->Close();
        if (!close_result.ok()) {
            return false;
        }
        
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

std::vector<RecordBatchPtr> StreamTable::load_persisted_data(int64_t offset, int64_t limit) const {
    std::vector<RecordBatchPtr> result;
    
    try {
        std::string file_path = persistence_dir_ + "/" + name_ + ".arrow";
        
        if (!std::filesystem::exists(file_path)) {
            return result;
        }
        
        // Open file for reading
        std::shared_ptr<arrow::io::ReadableFile> file;
        auto file_result = arrow::io::ReadableFile::Open(file_path);
        if (!file_result.ok()) {
            return result;
        }
        file = file_result.ValueOrDie();
        
        // Create IPC reader
        std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader;
        auto reader_result = arrow::ipc::RecordBatchFileReader::Open(file);
        if (!reader_result.ok()) {
            return result;
        }
        reader = reader_result.ValueOrDie();
        
        // Read batches
        int num_batches = reader->num_record_batches();
        int64_t current_row = 0;
        int64_t rows_collected = 0;
        
        for (int i = 0; i < num_batches; ++i) {
            auto batch_result = reader->ReadRecordBatch(i);
            if (!batch_result.ok()) {
                continue;
            }
            
            auto batch = batch_result.ValueOrDie();
            int64_t batch_rows = batch->num_rows();
            
            // Skip batches before offset
            if (current_row + batch_rows <= offset) {
                current_row += batch_rows;
                continue;
            }
            
            // Check if we've collected enough
            if (limit > 0 && rows_collected >= limit) {
                break;
            }
            
            // Add batch to result
            result.push_back(batch);
            rows_collected += batch_rows;
            current_row += batch_rows;
        }
        
    } catch (const std::exception& e) {
        // Log error
    }
    
    return result;
}

// KeyedStreamTable implementation
KeyedStreamTable::KeyedStreamTable(const std::string& name,
                                  const std::vector<std::string>& key_columns,
                                  SchemaPtr schema,
                                  const StreamConfig& config)
    : StreamTable(name, schema, config), key_columns_(key_columns) {
}

bool KeyedStreamTable::append(RecordBatchPtr batch) {
    if (!batch || batch->num_rows() == 0) {
        return false;
    }

    // Check for duplicate keys
    std::lock_guard<std::mutex> keys_lock(keys_mutex_);
    
    for (int64_t i = 0; i < batch->num_rows(); ++i) {
        std::string key = compute_key(batch, i);
        if (existing_keys_.find(key) != existing_keys_.end()) {
            // Duplicate key found, reject the batch
            return false;
        }
    }
    
    // No duplicates, add keys and append batch
    for (int64_t i = 0; i < batch->num_rows(); ++i) {
        std::string key = compute_key(batch, i);
        existing_keys_.insert(key);
    }
    
    return StreamTable::append(batch);
}

bool KeyedStreamTable::append(const std::vector<RecordBatchPtr>& batches) {
    // Check key constraints for all batches first
    for (const auto& batch : batches) {
        if (!batch || batch->num_rows() == 0) {
            continue;
        }
        
        // Check key constraint for each row in the batch
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            std::string key = compute_key(batch, i);
            
            std::lock_guard<std::mutex> lock(keys_mutex_);
            if (existing_keys_.find(key) != existing_keys_.end()) {
                return false; // Key constraint violation
            }
        }
    }
    
    // If all checks pass, add the keys and append the batches
    for (const auto& batch : batches) {
        if (!batch || batch->num_rows() == 0) {
            continue;
        }
        
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            std::string key = compute_key(batch, i);
            std::lock_guard<std::mutex> lock(keys_mutex_);
            existing_keys_.insert(key);
        }
    }
    
    return StreamTable::append(batches);
}

std::string KeyedStreamTable::compute_key(RecordBatchPtr batch, int64_t row_index) const {
    std::ostringstream key_stream;
    
    for (size_t i = 0; i < key_columns_.size(); ++i) {
        if (i > 0) {
            key_stream << "|";
        }
        
        const std::string& col_name = key_columns_[i];
        auto column = batch->GetColumnByName(col_name);
        if (column) {
            // This is a simplified key computation
            // In practice, you'd need to handle different data types properly
            key_stream << column->ToString();
        }
    }
    
    return key_stream.str();
}

} // namespace streaming_compute