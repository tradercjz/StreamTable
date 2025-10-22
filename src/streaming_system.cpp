#include "streaming_compute/streaming_compute.h"

namespace streaming_compute {

StreamingSystem::StreamingSystem(const StreamConfig& config) : config_(config) {
}

StreamingSystem::~StreamingSystem() {
    shutdown();
}

bool StreamingSystem::initialize() {
    if (running_) {
        return false;
    }

    // Initialize core components
    publisher_ = std::make_unique<Publisher>(config_);
    subscriber_ = std::make_unique<Subscriber>("system_subscriber", config_);
    
    // Initialize monitoring
    StreamingMonitor::MonitoringConfig monitor_config;
    monitor_config.enable_detailed_stats = true;
    monitor_config.stats_collection_interval_ms = 1000;
    monitor_config.enable_alerts = true;
    monitor_ = std::make_unique<StreamingMonitor>(monitor_config);
    
    // Initialize persistence manager if enabled
    if (config_.enable_persistence) {
        PersistenceManager::PersistenceConfig persistence_config;
        persistence_config.persistence_dir = config_.persistence_dir;
        persistence_config.enable_compression = true;
        persistence_config.async_write = true;
        
        persistence_manager_ = std::make_unique<PersistenceManager>(persistence_config);
        if (!persistence_manager_->initialize()) {
            return false;
        }
    }

    // Start components
    if (!publisher_->start()) {
        return false;
    }

    if (!subscriber_->start()) {
        publisher_->stop();
        return false;
    }

    // Start monitoring
    if (!monitor_->start()) {
        subscriber_->stop();
        publisher_->stop();
        return false;
    }

    register_components_for_monitoring();
    
    running_ = true;
    return true;
}

void StreamingSystem::shutdown() {
    if (!running_) {
        return;
    }

    running_ = false;

    // Stop monitoring
    if (monitor_) {
        monitor_->stop();
    }

    // Stop core components
    if (subscriber_) {
        subscriber_->stop();
    }

    if (publisher_) {
        publisher_->stop();
    }

    if (persistence_manager_) {
        persistence_manager_->shutdown();
    }

    // Clear managed objects
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    engines_.clear();
    tables_.clear();
}

std::shared_ptr<StreamTable> StreamingSystem::create_stream_table(
    const std::string& name,
    SchemaPtr schema,
    bool keyed,
    const std::vector<std::string>& key_columns) {
    
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    if (tables_.find(name) != tables_.end()) {
        return nullptr; // Table already exists
    }

    std::shared_ptr<StreamTable> table;
    
    if (keyed) {
        table = std::make_shared<KeyedStreamTable>(name, key_columns, schema, config_);
    } else {
        table = std::make_shared<StreamTable>(name, schema, config_);
    }

    tables_[name] = table;
    
    // Register for monitoring
    monitor_->register_table(table);
    
    return table;
}

bool StreamingSystem::share_stream_table(std::shared_ptr<StreamTable> table, 
                                        const std::string& shared_name) {
    if (!table || !publisher_) {
        return false;
    }

    table->share(shared_name);
    return publisher_->register_table(table);
}

bool StreamingSystem::drop_stream_table(const std::string& table_name) {
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return false;
    }

    auto table = it->second;
    tables_.erase(it);
    lock.unlock();

    // Unregister from publisher
    if (publisher_ && table->is_shared()) {
        publisher_->unregister_table(table_name);
    }

    // Unregister from persistence
    if (persistence_manager_) {
        persistence_manager_->unregister_table(table_name);
    }

    return true;
}

std::string StreamingSystem::subscribe_table(
    const std::string& server,
    const std::string& table_name,
    const std::string& action_name,
    int64_t offset,
    MessageHandler handler,
    bool msg_as_table,
    size_t batch_size,
    double throttle,
    bool reconnect) {
    
    if (!subscriber_) {
        return "";
    }

    return subscriber_->subscribe_table(server, table_name, action_name, offset,
                                       std::move(handler), msg_as_table, batch_size,
                                       throttle, reconnect);
}

bool StreamingSystem::unsubscribe_table(const std::string& server,
                                       const std::string& table_name,
                                       const std::string& action_name) {
    if (!subscriber_) {
        return false;
    }

    return subscriber_->unsubscribe_table(server, table_name, action_name);
}

std::shared_ptr<StreamEngine> StreamingSystem::create_time_series_engine(
    const std::string& name,
    const std::string& metrics,
    std::shared_ptr<StreamTable> dummy_table,
    std::shared_ptr<StreamTable> output_table,
    const std::string& key_column,
    size_t window_size) {
    
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    if (engines_.find(name) != engines_.end()) {
        return nullptr; // Engine already exists
    }

    auto engine = StreamEngineFactory::create_time_series_engine(
        name, metrics, dummy_table, output_table, key_column, window_size);
    
    engines_[name] = engine;
    
    // Register for monitoring
    monitor_->register_engine(engine);
    
    return engine;
}

std::shared_ptr<StreamEngine> StreamingSystem::create_reactive_state_engine(
    const std::string& name,
    const std::string& metrics,
    std::shared_ptr<StreamTable> dummy_table,
    std::shared_ptr<StreamTable> output_table,
    const std::string& key_column) {
    
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    if (engines_.find(name) != engines_.end()) {
        return nullptr; // Engine already exists
    }

    auto engine = StreamEngineFactory::create_reactive_state_engine(
        name, metrics, dummy_table, output_table, key_column);
    
    engines_[name] = engine;
    
    // Register for monitoring
    monitor_->register_engine(engine);
    
    return engine;
}

std::shared_ptr<StreamEngine> StreamingSystem::create_cross_sectional_engine(
    const std::string& name,
    const std::string& metrics,
    std::shared_ptr<StreamTable> dummy_table,
    std::shared_ptr<StreamTable> output_table,
    const std::string& key_column,
    const std::string& triggering_pattern,
    size_t triggering_interval) {
    
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    if (engines_.find(name) != engines_.end()) {
        return nullptr; // Engine already exists
    }

    auto engine = StreamEngineFactory::create_cross_sectional_engine(
        name, metrics, dummy_table, output_table, key_column, 
        triggering_pattern, triggering_interval);
    
    engines_[name] = engine;
    
    // Register for monitoring
    monitor_->register_engine(engine);
    
    return engine;
}

bool StreamingSystem::drop_stream_engine(const std::string& name) {
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = engines_.find(name);
    if (it == engines_.end()) {
        return false;
    }

    // Deactivate engine
    it->second->set_active(false);
    
    engines_.erase(it);
    return true;
}

std::shared_ptr<StreamEngine> StreamingSystem::get_stream_engine(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = engines_.find(name);
    if (it != engines_.end()) {
        return it->second;
    }
    
    return nullptr;
}

bool StreamingSystem::enable_table_persistence(std::shared_ptr<StreamTable> table,
                                              size_t cache_size,
                                              bool async_write,
                                              bool compress) {
    if (!table || !persistence_manager_) {
        return false;
    }

    // Enable persistence on the table
    table->enable_persistence(config_.persistence_dir, cache_size, async_write, compress);
    
    // Register with persistence manager
    return persistence_manager_->register_table(table);
}

void StreamingSystem::register_components_for_monitoring() {
    // Register publisher and subscriber
    if (publisher_) {
        monitor_->register_publisher(std::shared_ptr<Publisher>(publisher_.get(), [](Publisher*){}));
    }
    
    if (subscriber_) {
        monitor_->register_subscriber(std::shared_ptr<Subscriber>(subscriber_.get(), [](Subscriber*){}));
    }
    
    // Register existing tables and engines
    std::shared_lock<std::shared_mutex> lock(objects_mutex_);
    
    for (const auto& table_pair : tables_) {
        monitor_->register_table(table_pair.second);
    }
    
    for (const auto& engine_pair : engines_) {
        monitor_->register_engine(engine_pair.second);
    }
}

} // namespace streaming_compute