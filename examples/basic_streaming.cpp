/**
 * Basic Streaming Example
 * 
 * This example demonstrates the basic usage of the streaming compute library:
 * - Creating stream tables
 * - Publishing and subscribing to data
 * - Basic data processing
 */

#include "streaming_compute/streaming_compute.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <random>

using namespace streaming_compute;

// Create sample data
RecordBatchPtr create_sample_data(SchemaPtr schema, const std::string& symbol, double price, int64_t volume) {
    // Create timestamp array
    arrow::TimestampBuilder timestamp_builder(arrow::timestamp(arrow::TimeUnit::MILLI), 
                                             arrow::default_memory_pool());
    timestamp_builder.Append(current_timestamp());
    ArrayPtr timestamp_array;
    timestamp_builder.Finish(&timestamp_array);
    
    // Create symbol array
    arrow::StringBuilder symbol_builder;
    symbol_builder.Append(symbol);
    ArrayPtr symbol_array;
    symbol_builder.Finish(&symbol_array);
    
    // Create price array
    arrow::DoubleBuilder price_builder;
    price_builder.Append(price);
    ArrayPtr price_array;
    price_builder.Finish(&price_array);
    
    // Create volume array
    arrow::Int64Builder volume_builder;
    volume_builder.Append(volume);
    ArrayPtr volume_array;
    volume_builder.Finish(&volume_array);
    
    std::vector<ArrayPtr> arrays = {timestamp_array, symbol_array, price_array, volume_array};
    return arrow::RecordBatch::Make(schema, 1, arrays);
}

int main() {
    std::cout << "=== Basic Streaming Example ===" << std::endl;
    
    // Create streaming system
    StreamConfig config;
    config.max_memory_size = 10000;
    config.enable_persistence = false;
    
    StreamingSystem system(config);
    
    // Initialize system
    if (!system.initialize()) {
        std::cerr << "Failed to initialize streaming system" << std::endl;
        return 1;
    }
    
    std::cout << "Streaming system initialized successfully" << std::endl;
    
    // Create schema for market data
    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("symbol", arrow::utf8()),
        arrow::field("price", arrow::float64()),
        arrow::field("volume", arrow::int64())
    };
    auto schema = arrow::schema(fields);
    
    // Create stream tables
    auto pub_table = system.create_stream_table("market_data", schema);
    auto sub_table = system.create_stream_table("processed_data", schema);
    
    if (!pub_table || !sub_table) {
        std::cerr << "Failed to create stream tables" << std::endl;
        return 1;
    }
    
    std::cout << "Created stream tables: market_data, processed_data" << std::endl;
    
    // Share the publisher table
    if (!system.share_stream_table(pub_table, "market_data")) {
        std::cerr << "Failed to share publisher table" << std::endl;
        return 1;
    }
    
    std::cout << "Shared market_data table for publishing" << std::endl;
    
    // Set up subscription
    std::atomic<int> messages_received{0};
    
    auto message_handler = [&](const StreamMessage& message) {
        messages_received++;
        std::cout << "Received message " << messages_received.load() 
                  << " with " << message.batch->num_rows() << " rows" << std::endl;
        
        // Process the message (simple pass-through to output table)
        sub_table->append(message.batch);
    };
    
    std::string topic = system.subscribe_table("", "market_data", "basic_processor", 
                                              0, message_handler, true);
    
    if (topic.empty()) {
        std::cerr << "Failed to subscribe to market_data" << std::endl;
        return 1;
    }
    
    std::cout << "Subscribed to topic: " << topic << std::endl;
    
    // Simulate data publishing
    std::cout << "\nStarting data simulation..." << std::endl;
    
    std::vector<std::string> symbols = {"AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"};
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> symbol_dist(0, symbols.size() - 1);
    std::uniform_real_distribution<> price_dist(100.0, 200.0);
    std::uniform_int_distribution<> volume_dist(1000, 10000);
    
    // Publish 50 messages
    for (int i = 0; i < 50; ++i) {
        std::string symbol = symbols[symbol_dist(gen)];
        double price = price_dist(gen);
        int64_t volume = volume_dist(gen);
        
        auto batch = create_sample_data(schema, symbol, price, volume);
        pub_table->append(batch);
        
        std::cout << "Published: " << symbol << " $" << price << " vol:" << volume << std::endl;
        
        // Wait a bit between messages
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    // Wait for all messages to be processed
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // Print statistics
    std::cout << "\n=== Statistics ===" << std::endl;
    std::cout << "Messages published: 50" << std::endl;
    std::cout << "Messages received: " << messages_received.load() << std::endl;
    std::cout << "Publisher table size: " << pub_table->size() << std::endl;
    std::cout << "Subscriber table size: " << sub_table->size() << std::endl;
    
    // Get streaming statistics
    auto streaming_stats = system.get_streaming_stat();
    std::cout << "\nStreaming Statistics:" << std::endl;
    for (const auto& category : streaming_stats) {
        std::cout << "  " << category.first << ": " << category.second.size() << " entries" << std::endl;
    }
    
    // Get performance metrics
    auto perf_metrics = system.get_performance_metrics();
    std::cout << "\nPerformance Metrics:" << std::endl;
    for (const auto& metric : perf_metrics) {
        std::cout << "  " << metric.first << ": " << metric.second << std::endl;
    }
    
    // Unsubscribe
    system.unsubscribe_table("", "market_data", "basic_processor");
    std::cout << "\nUnsubscribed from market_data" << std::endl;
    
    // Shutdown system
    system.shutdown();
    std::cout << "Streaming system shutdown complete" << std::endl;
    
    return 0;
}