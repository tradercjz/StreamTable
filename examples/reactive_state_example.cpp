/**
 * Reactive State Engine Example
 * 
 * This example demonstrates the usage of ReactiveStateEngine for:
 * - Cumulative calculations
 * - Stateful computations
 * - Per-key state management
 */

#include "streaming_compute/streaming_compute.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <random>
#include <iomanip>

using namespace streaming_compute;

// Create sample trading data
RecordBatchPtr create_trade_data(SchemaPtr schema, const std::string& symbol, double price, int64_t quantity) {
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
    
    arrow::Int64Builder quantity_builder;
    quantity_builder.Append(quantity);
    ArrayPtr quantity_array;
    quantity_builder.Finish(&quantity_array);
    
    std::vector<ArrayPtr> arrays = {timestamp_array, symbol_array, price_array, quantity_array};
    return arrow::RecordBatch::Make(schema, 1, arrays);
}

int main() {
    std::cout << "=== Reactive State Engine Example ===" << std::endl;
    
    // Create streaming system
    StreamConfig config;
    config.max_memory_size = 10000;
    
    StreamingSystem system(config);
    
    if (!system.initialize()) {
        std::cerr << "Failed to initialize streaming system" << std::endl;
        return 1;
    }
    
    std::cout << "Streaming system initialized" << std::endl;
    
    // Create schema for trade data
    std::vector<std::shared_ptr<arrow::Field>> input_fields = {
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("symbol", arrow::utf8()),
        arrow::field("price", arrow::float64()),
        arrow::field("quantity", arrow::int64())
    };
    auto input_schema = arrow::schema(input_fields);
    
    // Create schema for reactive state results
    std::vector<std::shared_ptr<arrow::Field>> output_fields = {
        arrow::field("symbol", arrow::utf8()),
        arrow::field("cumsum_quantity", arrow::float64()),
        arrow::field("cumavg_price", arrow::float64()),
        arrow::field("cummax_price", arrow::float64()),
        arrow::field("cummin_price", arrow::float64()),
        arrow::field("cumcount", arrow::float64())
    };
    auto output_schema = arrow::schema(output_fields);
    
    // Create stream tables
    auto trade_stream = system.create_stream_table("trade_stream", input_schema);
    auto state_results = system.create_stream_table("state_results", output_schema);
    
    if (!trade_stream || !state_results) {
        std::cerr << "Failed to create stream tables" << std::endl;
        return 1;
    }
    
    std::cout << "Created trade stream and state results tables" << std::endl;
    
    // Create reactive state engine with cumulative metrics
    std::string metrics = "cumsum(quantity), cumavg(price), cummax(price), cummin(price), cumcount()";
    
    auto state_engine = system.create_reactive_state_engine(
        "trade_analytics", metrics, trade_stream, state_results, "symbol");
    
    if (!state_engine) {
        std::cerr << "Failed to create reactive state engine" << std::endl;
        return 1;
    }
    
    std::cout << "Created reactive state engine" << std::endl;
    std::cout << "Metrics: " << metrics << std::endl;
    
    // Share trade stream for publishing
    system.share_stream_table(trade_stream, "trade_stream");
    
    // Set up subscription to feed the engine
    auto engine_handler = [&](const StreamMessage& message) {
        state_engine->process_message(message);
    };
    
    std::string topic = system.subscribe_table("", "trade_stream", "state_engine_feed", 
                                              0, engine_handler, true);
    
    if (topic.empty()) {
        std::cerr << "Failed to subscribe to trade_stream" << std::endl;
        return 1;
    }
    
    std::cout << "Engine subscribed to trade stream" << std::endl;
    
    // Monitor state results
    std::atomic<int> state_updates{0};
    auto results_handler = [&](const StreamMessage& message) {
        state_updates++;
        std::cout << "State update " << state_updates.load() 
                  << " received with " << message.batch->num_rows() << " rows" << std::endl;
    };
    
    system.share_stream_table(state_results, "state_results");
    system.subscribe_table("", "state_results", "state_monitor", 
                          0, results_handler, true);
    
    // Simulate trading activity
    std::cout << "\nSimulating trading activity..." << std::endl;
    
    std::vector<std::string> symbols = {"AAPL", "GOOGL", "MSFT", "TSLA"};
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> price_dist(100.0, 200.0);
    std::uniform_int_distribution<> quantity_dist(100, 1000);
    
    // Track some statistics for display
    std::unordered_map<std::string, double> last_prices;
    std::unordered_map<std::string, int64_t> total_quantities;
    std::unordered_map<std::string, int> trade_counts;
    
    // Generate trades for each symbol
    for (int round = 0; round < 10; ++round) {
        std::cout << "\n--- Trading Round " << (round + 1) << " ---" << std::endl;
        
        for (const auto& symbol : symbols) {
            // Generate 1-3 trades per symbol per round
            int num_trades = 1 + (gen() % 3);
            
            for (int i = 0; i < num_trades; ++i) {
                double price = price_dist(gen);
                int64_t quantity = quantity_dist(gen);
                
                // Update our tracking
                last_prices[symbol] = price;
                total_quantities[symbol] += quantity;
                trade_counts[symbol]++;
                
                auto batch = create_trade_data(input_schema, symbol, price, quantity);
                trade_stream->append(batch);
                
                std::cout << "Trade: " << symbol << " $" << std::fixed << std::setprecision(2) 
                          << price << " qty:" << quantity 
                          << " (total qty: " << total_quantities[symbol] 
                          << ", trades: " << trade_counts[symbol] << ")" << std::endl;
                
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        
        // Wait for state processing
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
    
    // Wait for final processing
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // Print engine statistics
    std::cout << "\n=== Reactive State Engine Statistics ===" << std::endl;
    auto engine_stats = state_engine->get_stats();
    for (const auto& stat : engine_stats) {
        std::cout << "  " << stat.first << ": " << stat.second << std::endl;
    }
    
    // Print per-symbol state information
    std::cout << "\n=== Per-Symbol State Summary ===" << std::endl;
    for (const auto& symbol : symbols) {
        auto key_state = std::static_pointer_cast<ReactiveStateEngine>(state_engine)->get_key_state(symbol);
        
        std::cout << symbol << ":" << std::endl;
        std::cout << "  Last Price: $" << std::fixed << std::setprecision(2) << last_prices[symbol] << std::endl;
        std::cout << "  Total Quantity: " << total_quantities[symbol] << std::endl;
        std::cout << "  Trade Count: " << trade_counts[symbol] << std::endl;
        
        if (!key_state.empty()) {
            std::cout << "  Engine State:" << std::endl;
            for (const auto& state_pair : key_state) {
                std::cout << "    " << state_pair.first << ": " << state_pair.second << std::endl;
            }
        }
        std::cout << std::endl;
    }
    
    // Print table statistics
    std::cout << "=== Table Statistics ===" << std::endl;
    std::cout << "Trade stream size: " << trade_stream->size() << std::endl;
    std::cout << "State results size: " << state_results->size() << std::endl;
    std::cout << "State updates received: " << state_updates.load() << std::endl;
    
    // Test state persistence (if snapshots are enabled)
    std::cout << "\n=== Testing State Persistence ===" << std::endl;
    if (state_engine->enable_snapshot("./snapshots", 10)) {
        std::cout << "Snapshot enabled, creating snapshot..." << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        int64_t snapshot_msg_id = state_engine->get_snapshot_msg_id();
        std::cout << "Last snapshot message ID: " << snapshot_msg_id << std::endl;
    } else {
        std::cout << "Snapshot not available (directory creation may have failed)" << std::endl;
    }
    
    // Get system performance metrics
    auto perf_metrics = system.get_performance_metrics();
    std::cout << "\n=== Performance Metrics ===" << std::endl;
    for (const auto& metric : perf_metrics) {
        std::cout << "  " << metric.first << ": " << metric.second << std::endl;
    }
    
    std::cout << "\nReactive state analysis complete!" << std::endl;
    
    // Cleanup
    system.shutdown();
    
    return 0;
}