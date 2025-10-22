#pragma once

#include "stream_engine.h"
#include "common.h"
#include <memory>
#include <unordered_map>
#include <vector>
#include <string>
#include <functional>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace streaming_compute {

// Forward declarations
class CEPSubEngine;
class CEPMonitor;

/**
 * Event class - base class for all CEP events
 */
class Event {
public:
    Event() = default;
    virtual ~Event() = default;
    
    virtual std::string event_type() const = 0;
    virtual std::string to_string() const = 0;
    
    // Time handling
    virtual int64_t event_time() const { return event_time_; }
    virtual void set_event_time(int64_t time) { event_time_ = time; }
    
    // System time
    virtual int64_t system_time() const { return system_time_; }
    virtual void set_system_time(int64_t time) { system_time_ = time; }
    
private:
    int64_t event_time_ = 0;
    int64_t system_time_ = 0;
};

/**
 * Event schema definition
 */
struct EventSchema {
    std::string event_type;
    std::vector<std::string> field_names;
    std::vector<std::shared_ptr<arrow::DataType>> field_types;
    
    EventSchema(const std::string& type, 
                const std::vector<std::string>& names,
                const std::vector<std::shared_ptr<arrow::DataType>>& types)
        : event_type(type), field_names(names), field_types(types) {}
};

/**
 * CEP Monitor - base class for user-defined event handlers
 */
class CEPMonitor {
public:
    CEPMonitor() = default;
    virtual ~CEPMonitor() = default;
    
    // Lifecycle methods
    virtual void on_load() {}
    virtual void on_unload() {}
    virtual void on_destroy() {}
    
    // Event routing methods
    void send_event(std::shared_ptr<Event> event);
    void route_event(std::shared_ptr<Event> event);
    void emit_event(std::shared_ptr<Event> event, const std::string& event_time_field = "");
    
    // Sub-engine control
    void stop_sub_engine();
    
    // Event listener registration
    template<typename EventType, typename HandlerType>
    void add_event_listener(HandlerType handler, 
                          const std::string& event_type = "",
                          const std::string& filter = "all",
                          int priority = 0,
                          int64_t timeout_ms = 0,
                          int64_t duration_ms = 0);
    
    // Get sub-engine
    std::shared_ptr<CEPSubEngine> get_sub_engine() const { return sub_engine_.lock(); }
    
protected:
    std::weak_ptr<CEPSubEngine> sub_engine_;
    
    friend class CEPSubEngine;
};

/**
 * CEP Sub-Engine - individual processing unit
 */
class CEPSubEngine : public std::enable_shared_from_this<CEPSubEngine> {
public:
    CEPSubEngine(const std::string& name, 
                 const std::vector<std::shared_ptr<CEPMonitor>>& monitors,
                 size_t event_queue_depth = 1024);
    ~CEPSubEngine();
    
    // Event processing
    bool append_event(std::shared_ptr<Event> event);
    void process_events();
    void stop();
    
    // Event routing
    void send_event(std::shared_ptr<Event> event);
    void route_event(std::shared_ptr<Event> event);
    void emit_event(std::shared_ptr<Event> event, const std::string& event_time_field = "");
    
    // Getters
    const std::string& name() const { return name_; }
    bool is_running() const { return running_; }
    size_t queue_size() const { return event_queue_.size(); }
    
private:
    std::string name_;
    std::vector<std::shared_ptr<CEPMonitor>> monitors_;
    std::queue<std::shared_ptr<Event>> event_queue_;
    size_t event_queue_depth_;
    
    std::atomic<bool> running_{false};
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::thread processing_thread_;
    
    void initialize_monitors();
    void cleanup_monitors();
    void process_single_event(std::shared_ptr<Event> event);
};

/**
 * CEP Engine - main complex event processing engine
 */
class CEPEngine : public StreamEngine {
public:
    /**
     * Constructor
     */
    CEPEngine(const std::string& name,
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
    
    ~CEPEngine();
    
    // StreamEngine interface implementation
    bool process_message(const StreamMessage& message) override;
    
    // CEP-specific methods
    bool append_event(std::shared_ptr<Event> event);
    bool append_event_dict(const std::unordered_map<std::string, std::any>& event_dict);
    
    // Sub-engine management
    std::shared_ptr<CEPSubEngine> get_sub_engine(const std::string& key);
    void stop_all_sub_engines();
    
    // Statistics
    std::unordered_map<std::string, int64_t> get_stats() const override;
    
private:
    std::vector<std::shared_ptr<CEPMonitor>> monitors_;
    std::vector<EventSchema> event_schemas_;
    std::string dispatch_key_;
    size_t dispatch_bucket_;
    std::string time_column_;
    std::string event_time_field_;
    bool use_system_time_;
    size_t event_queue_depth_;
    size_t deserialize_parallelism_;
    
    // Sub-engine management
    std::unordered_map<std::string, std::shared_ptr<CEPSubEngine>> sub_engines_;
    mutable std::shared_mutex sub_engines_mutex_;
    
    // Helper methods
    std::string get_sub_engine_key(const std::unordered_map<std::string, std::any>& event_dict) const;
    std::string get_sub_engine_key(std::shared_ptr<Event> event) const;
    std::shared_ptr<Event> create_event_from_dict(const std::unordered_map<std::string, std::any>& event_dict) const;
    std::shared_ptr<Event> create_event_from_message(const StreamMessage& message) const;
    
    // Event processing threads
    std::vector<std::thread> deserialize_threads_;
    std::atomic<bool> running_{false};
    
    void start_deserialize_threads();
    void stop_deserialize_threads();
    void deserialize_worker(int thread_id);
};

/**
 * CEP Engine Factory
 */
class CEPEngineFactory {
public:
    /**
     * Create a CEP engine
     */
    static std::shared_ptr<StreamEngine> create_cep_engine(
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
};

} // namespace streaming_compute