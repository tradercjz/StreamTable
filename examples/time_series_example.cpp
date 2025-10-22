/**
 * Time Series Engine Example
 * 
 * This example demonstrates the usage of TimeSeriesEngine for:
 * - Moving window calculations
 * - Time-based aggregations
 * - Multiple metrics computation
 */

#include "streaming_compute/streaming_compute.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <random>
#include <iomanip>

using namespace streaming_compute;

// Create sample market data
RecordBatchPtr create_market_data(SchemaPtr schema, const std::string& symbol, double price, int64_t volume) {
    arrow::TimestampBuilder timestamp_builder(arrow::timestamp(arrow::TimeUnit::MILLI), 
                                             arrow::default_memory_pool());
    timestamp_builder.Append(current_timestamp());
    ArrayPtr timestamp_array;
    timestamp_builder.Finish(&timestamp_array);
    
    arrow::StringBuilder symbol_builder;
    symbol_builder.Append(symbol);
    ArrayPtr symbol_array;
    symbol_builder.Finish(&symbol_array);
    
    arrow::DoubleBuilder price_builder;
    price_builder.Append(price);
    ArrayPtr price_array;
    price_builder.Finish(&price_array);
    
    arrow::Int64Builder volume_builder;
    volume_builder.Append(volume);
    ArrayPtr volume_array;
    volume_builder.Finish(&volume_array);
    
    std::vector<ArrayPtr> arrays = {timestamp_array, symbol_array, price_array, volume_array};
    return arrow::RecordBatch::Make(schema, 1, arrays);
}

int main() {
    std::cout << "=== Time Series Engine Example ===" << std::endl;
    
    // Create streaming system
    StreamConfig config;
    config.max_memory_size = 10000;
    
    StreamingSystem system(config);
    
    if (!system.initialize()) {
        std::cerr << "Failed to initialize streaming system" << std::endl;
        return 1;
    }
    
    std::cout << "Streaming system initialized" << std::endl;
    
    // Create schema for market data
    std::vector<std::shared_ptr<arrow::Field>> input_fields = {
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("symbol", arrow::utf8()),
        arrow::field("price", arrow::float64()),
        arrow::field("volume", arrow::int64())
    };
    auto input_schema = arrow::schema(input_fields);
    
    // Create schema for time series results
    std::vector<std::shared_ptr<arrow::Field>> output_fields = {
        arrow::field("symbol", arrow::utf8()),
        arrow::field("avg_price", arrow::float64()),
        arrow::field("sum_volume", arrow::float64()),
        arrow::field("min_price", arrow::float64()),
        arrow::field("max_price", arrow::float64()),
        arrow::field("count", arrow::float64())
    };
    auto output_schema = arrow::schema(output_fields);
    
    // Create stream tables
    auto input_table = system.create_stream_table("tick_stream", input_schema);
    auto output_table = system.create_stream_table("ts_results", output_schema);
    
    if (!input_table || !output_table) {
        std::cerr << "Failed to create stream tables" << std::endl;
        return 1;
    }
    
    std::cout << "Created input and output tables" << std::endl;
    
    // Create time series engine with multiple metrics
    std::string metrics = "avg(price), sum(volume), min(price), max(price), count()";
    size_t window_size = 10; // 10-record moving window
    
    auto ts_engine = system.create_time_series_engine(
        "price_analytics", metrics, input_table, output_table, "symbol", window_size);
    
    if (!ts_engine) {
        std::cerr << "Failed to create time series engine" << std::endl;
        return 1;
    }
    
    std::cout << "Created time series engine with window size: " << window_size << std::endl;
    std::cout << "Metrics: " << metrics << std::endl;
    
    // Share input table for publishing
    system.share_stream_table(input_table, "tick_stream");
    
    // Set up subscription to feed the engine
    auto engine_handler = [&](const StreamMessage& message) {
        ts_engine->process_message(message);
    };
    
    std::string topic = system.subscribe_table("", "tick_stream", "ts_engine_feed", 
                                              0, engine_handler, true);
    
    if (topic.empty()) {
        std::cerr << "Failed to subscribe to tick_stream" << std::endl;
        return 1;
    }
    
    std::cout << "Engine subscribed to input stream" << std::endl;
    
    // Monitor output table
    std::atomic<int> results_received{0};
    auto results_handler = [&](const StreamMessage& message) {
        results_received++;
        std::cout << "Time series result " << results_received.load() 
                  << " received with " << message.batch->num_rows() << " rows" << std::endl;
    };
    
    system.share_stream_table(output_table, "ts_results");
    system.subscribe_table("", "ts_results", "results_monitor", 
                          0, results_handler, true);
    
    // Simulate market data
    std::cout << "\nSimulating market data..." << std::endl;
    
    std::vector<std::string> symbols = {"AAPL", "GOOGL", "MSFT"};
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> symbol_dist(0, symbols.size() - 1);
    std::uniform_real_distribution<> price_dist(100.0, 200.0);
    std::uniform_int_distribution<> volume_dist(1000, 10000);
    
    // Generate data for each symbol to trigger window calculations
    for (int round = 0; round < 5; ++round) {
        std::cout << "\n--- Round " << (round + 1) << " ---" << std::endl;
        
        for (const auto& symbol : symbols) {
            // Generate multiple ticks per symbol to fill the window
            for (int i = 0; i < 3; ++i) {
                double price = price_dist(gen);
                int64_t volume = volume_dist(gen);
                
                auto batch = create_market_data(input_schema, symbol, price, volume);
                input_table->append(batch);
                
                std::cout << "Published: " << symbol << " $" << std::fixed << std::setprecision(2) 
                          << price << " vol:" << volume << std::endl;
                
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
        
        // Wait for processing
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    
    // Wait for final processing
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // Print engine statistics
    std::cout << "\n=== Time Series Engine Statistics ===" << std::endl;
    auto engine_stats = ts_engine->get_stats();
    for (const auto& stat : engine_stats) {
        std::cout << "  " << stat.first << ": " << stat.second << std::endl;
    }
    
    // Print table statistics
    std::cout << "\n=== Table Statistics ===" << std::endl;
    std::cout << "Input table size: " << input_table->size() << std::endl;
    std::cout << "Output table size: " << output_table->size() << std::endl;
    std::cout << "Results received: " << results_received.load() << std::endl;
    
    // Get streaming statistics
    auto streaming_stats = system.get_streaming_stat();
    std::cout << "\n=== System Statistics ===" << std::endl;
    for (const auto& category : streaming_stats) {
        std::cout << "  " << category.first << ": " << category.second.size() << " entries" << std::endl;
    }
    
    // Get engine-specific statistics
    auto engine_system_stats = system.get_stream_engine_stat();
    std::cout << "\n=== Engine System Statistics ===" << std::endl;
    for (const auto& engine_type : engine_system_stats) {
        std::cout << "  " << engine_type.first << ": " << engine_type.second.size() << " engines" << std::endl;
    }
    
    std::cout << "\nTime series analysis complete!" << std::endl;
    
    // Cleanup
    system.shutdown();
    
    return 0;
}