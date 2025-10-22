#pragma once

#include "common.h"

namespace streaming_compute {

/**
 * Subscriber - Receives and processes streaming data
 * 
 * Features:
 * - Topic subscription management
 * - Message filtering and batching
 * - Automatic reconnection
 * - Offset management
 * - Statistics collection
 */
class Subscriber {
public:
    /**
     * Constructor
     * @param subscriber_id Unique identifier for this subscriber
     * @param config Configuration parameters
     */
    explicit Subscriber(const std::string& subscriber_id, const StreamConfig& config = StreamConfig{});
    
    /**
     * Destructor
     */
    ~Subscriber();

    /**
     * Subscribe to a topic
     * @param server Server address (empty for local)
     * @param table_name Table name to subscribe to
     * @param action_name Action name for this subscription
     * @param offset Starting offset (-1 for new messages only)
     * @param handler Message handler function
     * @param msg_as_table Whether to deliver messages as tables
     * @param batch_size Batch size for message delivery (0 for immediate)
     * @param throttle Throttle time in seconds
     * @param reconnect Whether to automatically reconnect
     * @return Topic string if successful, empty string otherwise
     */
    std::string subscribe_table(const std::string& server,
                               const std::string& table_name,
                               const std::string& action_name,
                               int64_t offset,
                               MessageHandler handler,
                               bool msg_as_table = false,
                               size_t batch_size = 0,
                               double throttle = 1.0,
                               bool reconnect = false);

    /**
     * Subscribe to a topic with batch handler
     * @param server Server address (empty for local)
     * @param table_name Table name to subscribe to
     * @param action_name Action name for this subscription
     * @param offset Starting offset (-1 for new messages only)
     * @param handler Batch handler function
     * @param batch_size Batch size for message delivery
     * @param throttle Throttle time in seconds
     * @param reconnect Whether to automatically reconnect
     * @return Topic string if successful, empty string otherwise
     */
    std::string subscribe_table_batch(const std::string& server,
                                     const std::string& table_name,
                                     const std::string& action_name,
                                     int64_t offset,
                                     BatchHandler handler,
                                     size_t batch_size = 1000,
                                     double throttle = 1.0,
                                     bool reconnect = false);

    /**
     * Unsubscribe from a topic
     * @param server Server address
     * @param table_name Table name
     * @param action_name Action name
     * @param remove_offset Whether to remove stored offset
     * @return true if successful, false otherwise
     */
    bool unsubscribe_table(const std::string& server,
                          const std::string& table_name,
                          const std::string& action_name,
                          bool remove_offset = true);

    /**
     * Get subscription statistics
     * @return Vector of subscription statistics
     */
    std::vector<SubscriptionStats> get_subscription_stats() const;

    /**
     * Get worker statistics
     * @return Vector of worker statistics
     */
    std::vector<WorkerStats> get_worker_stats() const;

    /**
     * Get processed offset for a topic
     * @param topic Topic name
     * @return Processed offset, -1 if not found
     */
    int64_t get_processed_offset(const std::string& topic) const;

    /**
     * Set processed offset for a topic
     * @param topic Topic name
     * @param offset Offset to set
     */
    void set_processed_offset(const std::string& topic, int64_t offset);

    /**
     * Remove stored offset for a topic
     * @param topic Topic name
     */
    void remove_topic_offset(const std::string& topic);

    /**
     * Start the subscriber
     * @return true if successful, false otherwise
     */
    bool start();

    /**
     * Stop the subscriber
     */
    void stop();

    /**
     * Check if the subscriber is running
     * @return true if running, false otherwise
     */
    bool is_running() const { return running_; }

private:
    struct Subscription {
        std::string topic;
        std::string server;
        std::string table_name;
        std::string action_name;
        int64_t offset;
        MessageHandler message_handler;
        BatchHandler batch_handler;
        bool msg_as_table;
        size_t batch_size;
        double throttle;
        bool reconnect;
        bool active;
        
        // Statistics
        std::atomic<int64_t> messages_received{0};
        std::atomic<int64_t> messages_processed{0};
        std::atomic<int64_t> messages_failed{0};
        std::chrono::system_clock::time_point last_message_time;
        std::chrono::system_clock::time_point subscription_start_time;
        
        // Message batching
        std::vector<StreamMessage> message_batch;
        std::mutex batch_mutex;
        std::chrono::system_clock::time_point last_batch_time;
        
        Subscription(const std::string& t, const std::string& s, const std::string& tn,
                    const std::string& an, int64_t off, MessageHandler mh, BatchHandler bh,
                    bool mat, size_t bs, double th, bool rc)
            : topic(t), server(s), table_name(tn), action_name(an), offset(off),
              message_handler(std::move(mh)), batch_handler(std::move(bh)),
              msg_as_table(mat), batch_size(bs), throttle(th), reconnect(rc), active(true),
              last_message_time(std::chrono::system_clock::now()),
              subscription_start_time(std::chrono::system_clock::now()),
              last_batch_time(std::chrono::system_clock::now()) {}
    };

    std::string subscriber_id_;
    StreamConfig config_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};
    
    // Subscriptions
    std::unordered_map<std::string, std::shared_ptr<Subscription>> subscriptions_;
    mutable std::shared_mutex subscriptions_mutex_;
    
    // Offset management
    std::unordered_map<std::string, int64_t> topic_offsets_;
    mutable std::mutex offsets_mutex_;
    
    // Worker threads
    std::vector<std::unique_ptr<std::thread>> worker_threads_;
    std::queue<std::function<void()>> task_queue_;
    std::mutex task_queue_mutex_;
    std::condition_variable task_condition_;
    
    // Statistics
    std::atomic<int64_t> total_messages_received_{0};
    std::atomic<int64_t> total_messages_processed_{0};
    std::atomic<int64_t> total_messages_failed_{0};
    std::atomic<int64_t> total_reconnections_{0};
    
    // Private methods
    void worker_thread();
    void process_subscription(std::shared_ptr<Subscription> subscription);
    void handle_message(std::shared_ptr<Subscription> subscription, const StreamMessage& message);
    void process_batch(std::shared_ptr<Subscription> subscription);
    void attempt_reconnection(std::shared_ptr<Subscription> subscription);
    bool connect_to_publisher(const std::string& server);
    void update_statistics(std::shared_ptr<Subscription> subscription, bool success);
    std::string generate_topic(const std::string& server, const std::string& table_name, 
                              const std::string& action_name) const;
};

} // namespace streaming_compute