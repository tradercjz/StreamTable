#include "streaming_compute/stream_engine.h"
#include "streaming_compute/time_series_engine.h"
#include "streaming_compute/reactive_state_engine.h"
#include "streaming_compute/cross_sectional_engine.h"
#include <filesystem>
#include <fstream>
#include <sstream>

namespace streaming_compute {

StreamEngine::StreamEngine(const std::string& name,
                          std::shared_ptr<StreamTable> dummy_table,
                          std::shared_ptr<StreamTable> output_table,
                          const std::string& key_column)
    : name_(name), dummy_table_(std::move(dummy_table)), 
      output_table_(std::move(output_table)), key_column_(key_column) {
}

size_t StreamEngine::process_batch(const std::vector<StreamMessage>& messages) {
    size_t processed = 0;
    
    for (const auto& message : messages) {
        if (!active_) {
            break;
        }
        
        if (process_message(message)) {
            processed++;
        }
    }
    
    return processed;
}

bool StreamEngine::enable_snapshot(const std::string& snapshot_dir, 
                                  size_t snapshot_interval_msg_count) {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    
    // Create snapshot directory if it doesn't exist
    if (!std::filesystem::create_directories(snapshot_dir) && 
        !std::filesystem::exists(snapshot_dir)) {
        return false;
    }
    
    snapshot_dir_ = snapshot_dir;
    snapshot_interval_msg_count_ = snapshot_interval_msg_count;
    snapshot_enabled_ = true;
    
    return true;
}

void StreamEngine::disable_snapshot() {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    snapshot_enabled_ = false;
}

int64_t StreamEngine::get_snapshot_msg_id() const {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    
    if (!snapshot_enabled_) {
        return -1;
    }
    
    std::string snapshot_path = get_snapshot_path();
    if (!std::filesystem::exists(snapshot_path)) {
        return -1;
    }
    
    return last_snapshot_msg_id_.load();
}

bool StreamEngine::load_snapshot(const std::string& snapshot_path) {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    
    if (!std::filesystem::exists(snapshot_path)) {
        return false;
    }
    
    try {
        std::ifstream file(snapshot_path, std::ios::binary);
        if (!file.is_open()) {
            return false;
        }
        
        // Read header
        int64_t msg_id;
        file.read(reinterpret_cast<char*>(&msg_id), sizeof(msg_id));
        
        if (file.fail()) {
            return false;
        }
        
        // Deserialize state
        if (!deserialize_state(file)) {
            return false;
        }
        
        last_snapshot_msg_id_ = msg_id;
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

bool StreamEngine::save_snapshot(const std::string& snapshot_path) {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    
    try {
        std::ofstream file(snapshot_path, std::ios::binary);
        if (!file.is_open()) {
            return false;
        }
        
        // Write header
        int64_t msg_id = last_processed_msg_id_.load();
        file.write(reinterpret_cast<const char*>(&msg_id), sizeof(msg_id));
        
        if (file.fail()) {
            return false;
        }
        
        // Serialize state
        if (!serialize_state(file)) {
            return false;
        }
        
        last_snapshot_msg_id_ = msg_id;
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

std::unordered_map<std::string, int64_t> StreamEngine::get_stats() const {
    std::unordered_map<std::string, int64_t> stats;
    
    stats["processed_messages"] = processed_messages_.load();
    stats["failed_messages"] = failed_messages_.load();
    stats["last_processed_msg_id"] = last_processed_msg_id_.load();
    stats["last_snapshot_msg_id"] = last_snapshot_msg_id_.load();
    stats["is_active"] = active_.load() ? 1 : 0;
    stats["snapshot_enabled"] = snapshot_enabled_ ? 1 : 0;
    
    return stats;
}

void StreamEngine::reset() {
    std::unique_lock<std::shared_mutex> lock(state_mutex_);
    
    processed_messages_ = 0;
    failed_messages_ = 0;
    last_processed_msg_id_ = -1;
    last_snapshot_msg_id_ = -1;
}

void StreamEngine::update_statistics(bool success) {
    if (success) {
        processed_messages_++;
    } else {
        failed_messages_++;
    }
}

bool StreamEngine::should_create_snapshot() const {
    if (!snapshot_enabled_) {
        return false;
    }
    
    int64_t processed = processed_messages_.load();
    int64_t last_snapshot = last_snapshot_msg_id_.load();
    
    return (processed - last_snapshot) >= static_cast<int64_t>(snapshot_interval_msg_count_);
}

void StreamEngine::create_snapshot_if_needed() {
    if (should_create_snapshot()) {
        std::string snapshot_path = get_snapshot_path();
        save_snapshot(snapshot_path);
    }
}

std::string StreamEngine::get_snapshot_path() const {
    return snapshot_dir_ + "/" + name_ + ".snapshot";
}

bool StreamEngine::serialize_state(std::ostream& out) const {
    // Base implementation - derived classes should override
    int64_t processed = processed_messages_.load();
    int64_t failed = failed_messages_.load();
    
    out.write(reinterpret_cast<const char*>(&processed), sizeof(processed));
    out.write(reinterpret_cast<const char*>(&failed), sizeof(failed));
    
    return !out.fail();
}

bool StreamEngine::deserialize_state(std::istream& in) {
    // Base implementation - derived classes should override
    int64_t processed, failed;
    
    in.read(reinterpret_cast<char*>(&processed), sizeof(processed));
    in.read(reinterpret_cast<char*>(&failed), sizeof(failed));
    
    if (in.fail()) {
        return false;
    }
    
    processed_messages_ = processed;
    failed_messages_ = failed;
    
    return true;
}

// StreamEngineFactory implementation
std::shared_ptr<StreamEngine> StreamEngineFactory::create_time_series_engine(
    const std::string& name,
    const std::string& metrics,
    std::shared_ptr<StreamTable> dummy_table,
    std::shared_ptr<StreamTable> output_table,
    const std::string& key_column,
    size_t window_size) {
    
    return std::make_shared<TimeSeriesEngine>(name, metrics, dummy_table, output_table, 
                                             key_column, window_size);
}

std::shared_ptr<StreamEngine> StreamEngineFactory::create_reactive_state_engine(
    const std::string& name,
    const std::string& metrics,
    std::shared_ptr<StreamTable> dummy_table,
    std::shared_ptr<StreamTable> output_table,
    const std::string& key_column) {
    
    return std::make_shared<ReactiveStateEngine>(name, metrics, dummy_table, output_table, 
                                                key_column);
}

std::shared_ptr<StreamEngine> StreamEngineFactory::create_cross_sectional_engine(
    const std::string& name,
    const std::string& metrics,
    std::shared_ptr<StreamTable> dummy_table,
    std::shared_ptr<StreamTable> output_table,
    const std::string& key_column,
    const std::string& triggering_pattern,
    size_t triggering_interval) {
    
    return std::make_shared<CrossSectionalEngine>(name, metrics, dummy_table, output_table,
                                                 key_column, triggering_pattern, 
                                                 triggering_interval);
}

std::shared_ptr<StreamEngine> StreamEngineFactory::create_anomaly_detection_engine(
    const std::string& name,
    const std::string& metrics,
    std::shared_ptr<StreamTable> dummy_table,
    std::shared_ptr<StreamTable> output_table,
    const std::string& key_column,
    double threshold) {
    
    // For now, return a reactive state engine as placeholder
    // In practice, you'd implement a dedicated AnomalyDetectionEngine
    return std::make_shared<ReactiveStateEngine>(name, metrics, dummy_table, output_table, 
                                                key_column);
}

} // namespace streaming_compute