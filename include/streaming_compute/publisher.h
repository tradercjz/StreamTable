#pragma once

#include "common.h"
#include "stream_table.h"
#include <unordered_set>

namespace streaming_compute {

/**
 * Publisher - Manages data distribution to multiple subscribers
 * 
 * Features:
 * - Topic-based publishing
 * - Multiple subscriber support
 * - Message queuing and batching
 * - Connection management
 * - Statistics collection
 */
class Publisher {
public:
    /**
     * Constructor
     * @param config Configuration parameters
     */
    explicit Publisher(const StreamConfig& config = StreamConfig{});
    
    /**
     * Destructor
     */
    ~Publisher();

    /**
     * Register a StreamTable for publishing
     * @param table Shared pointer to the StreamTable
     * @return true if successful, false otherwise
     */
    bool register_table(std::shared_ptr<StreamTable> table);

    /**
     * Unregister a StreamTable
     * @param table_name Name of the table to unregister
     * @return true if successful, false otherwise
     */
    bool unregister_table(const std::string& table_name);

    /**
     * Publish a message to all subscribers of a topic
     * @param topic Topic name
     * @param message Message to publish
     * @return Number of subscribers that received the message
     */
    size_t publish(const std::string& topic, const StreamMessage& message);

    /**
     * Add a subscriber to a topic
     * @param topic Topic name
     * @param subscriber_id Unique subscriber identifier
     * @param handler Message handler function
     * @param offset Starting offset for the subscription
     * @return true if successful, false otherwise
     */
    bool add_subscriber(const std::string& topic, 
                       const std::string& subscriber_id,
                       MessageHandler handler,
                       int64_t offset = -1);

    /**
     * Remove a subscriber from a topic
     * @param topic Topic name
     * @param subscriber_id Subscriber identifier
     * @return true if successful, false otherwise
     */
    bool remove_subscriber(const std::string& topic, const std::string& subscriber_id);

    /**
     * Set filter column for a table
     * @param table_name Table name
     * @param column_name Column to filter on
     * @return true if successful, false otherwise
     */
    bool set_filter_column(const std::string& table_name, const std::string& column_name);

    /**
     * Get connection statistics
     * @return Vector of connection statistics
     */
    std::vector<ConnectionStats> get_connection_stats() const;

    /**
     * Get publisher statistics
     * @return Map of statistics
     */
    std::unordered_map<std::string, int64_t> get_stats() const;

    /**
     * Start the publisher
     * @return true if successful, false otherwise
     */
    bool start();

    /**
     * Stop the publisher
     */
    void stop();

    /**
     * Check if the publisher is running
     * @return true if running, false otherwise
     */
    bool is_running() const { return running_; }

private:
    struct SubscriberInfo {
        std::string id;
        std::string topic;
        MessageHandler handler;
        int64_t offset;
        std::queue<StreamMessage> message_queue;
        std::mutex queue_mutex;
        std::atomic<int64_t> messages_sent{0};
        std::atomic<int64_t> messages_failed{0};
        std::chrono::system_clock::time_point last_activity;
        
        SubscriberInfo(const std::string& sub_id, const std::string& t, 
                      MessageHandler h, int64_t off)
            : id(sub_id), topic(t), handler(std::move(h)), offset(off),
              last_activity(std::chrono::system_clock::now()) {}
    };

    StreamConfig config_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};
    
    // Tables and topics
    std::unordered_map<std::string, std::shared_ptr<StreamTable>> tables_;
    std::unordered_map<std::string, std::string> table_filter_columns_;
    mutable std::shared_mutex tables_mutex_;
    
    // Subscribers
    std::unordered_map<std::string, std::vector<std::shared_ptr<SubscriberInfo>>> topic_subscribers_;
    mutable std::shared_mutex subscribers_mutex_;
    
    // Worker threads
    std::vector<std::unique_ptr<std::thread>> worker_threads_;
    std::queue<std::function<void()>> task_queue_;
    std::mutex task_queue_mutex_;
    std::condition_variable task_condition_;
    
    // Statistics
    std::atomic<int64_t> total_messages_published_{0};
    std::atomic<int64_t> total_subscribers_{0};
    std::atomic<int64_t> total_failed_deliveries_{0};
    
    // Private methods
    void worker_thread();
    void process_table_updates();
    void deliver_message(std::shared_ptr<SubscriberInfo> subscriber, const StreamMessage& message);
    bool apply_filter(const StreamMessage& message, const std::string& filter_column) const;
    void cleanup_inactive_subscribers();
    void handle_table_message(const std::string& table_name, const StreamMessage& message);
};

} // namespace streaming_compute