#pragma once

#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <queue>
#include <unordered_map>
#include <unordered_set>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

namespace streaming_compute {

// Type aliases for convenience
using RecordBatchPtr = std::shared_ptr<arrow::RecordBatch>;
using SchemaPtr = std::shared_ptr<arrow::Schema>;
using ArrayPtr = std::shared_ptr<arrow::Array>;
using TablePtr = std::shared_ptr<arrow::Table>;

// Forward declarations
class StreamTable;
class Publisher;
class Subscriber;
class StreamEngine;

// Configuration structure
struct StreamConfig {
    size_t max_memory_size = 10000;  // Maximum rows in memory
    size_t cache_size = 1000000;     // Cache size for persistence
    bool enable_persistence = false; // Enable disk persistence
    std::string persistence_dir = "./data"; // Persistence directory
    size_t batch_size = 1024;        // Default batch size
    double throttle_seconds = 1.0;   // Throttle time in seconds
    size_t max_queue_depth = 10000000; // Maximum queue depth
};

// Message structure for streaming
struct StreamMessage {
    int64_t offset;
    int64_t timestamp;
    RecordBatchPtr batch;
    std::string topic;
    
    StreamMessage(int64_t off, int64_t ts, RecordBatchPtr b, const std::string& t)
        : offset(off), timestamp(ts), batch(std::move(b)), topic(t) {}
};

// Handler function type
using MessageHandler = std::function<void(const StreamMessage&)>;
using BatchHandler = std::function<void(const std::vector<StreamMessage>&)>;

// Subscription information
struct SubscriptionInfo {
    std::string action_name;
    std::string table_name;
    int64_t offset;
    MessageHandler handler;
    BatchHandler batch_handler;
    bool msg_as_table;
    size_t batch_size;
    double throttle;
    bool reconnect;
    std::atomic<bool> active{true};
};

// Statistics structures
struct ConnectionStats {
    std::string client;
    size_t queue_depth_limit;
    size_t queue_depth;
    std::string tables;
};

struct SubscriptionStats {
    std::string publisher;
    int64_t cum_msg_count;
    double cum_msg_latency;
    double last_msg_latency;
    std::chrono::system_clock::time_point last_update;
};

struct WorkerStats {
    int worker_id;
    std::string topic;
    size_t queue_depth_limit;
    size_t queue_depth;
    int64_t processed_msg_count;
    int64_t failed_msg_count;
    std::string last_error_msg;
};

// Utility functions
inline int64_t current_timestamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

inline std::string generate_topic(const std::string& node, const std::string& table, 
                                 const std::string& action = "") {
    if (action.empty()) {
        return node + "/" + table;
    }
    return node + "/" + table + "/" + action;
}

} // namespace streaming_compute