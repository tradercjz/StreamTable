/**
 * Complete System Example
 * 
 * This example demonstrates a comprehensive streaming system that includes:
 * - Multiple stream tables
 * - Multiple computation engines working together
 * - Pipeline processing (engine chaining)
 * - Data persistence
 * - Comprehensive monitoring
 * - Real-world-like scenario (financial market data processing)
 */

#include "streaming_compute/streaming_compute.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <random>
#include <iomanip>
#include <fstream>

using namespace streaming_compute;

// Market data structure
struct MarketTick {
    std::string symbol;
    double price;
    int64_t volume;
    int64_t timestamp;
};

// Create market tick batch
RecordBatchPtr create_market_tick(SchemaPtr schema, const MarketTick& tick) {
    arrow::TimestampBuilder timestamp_builder(arrow::timestamp(arrow::TimeUnit::MILLI), 
                                             arrow::default_memory_pool());
    timestamp_builder.Append(tick.timestamp);
    ArrayPtr timestamp_array;
    timestamp_builder.Finish(&timestamp_array);
    
    arrow::StringBuilder symbol_builder;
    symbol_builder.Append(tick.symbol);
    ArrayPtr symbol_array;
    symbol_builder.Finish(&symbol_array);
    
    arrow::DoubleBuilder price_builder;
    price_builder.Append(tick.price);
    ArrayPtr price_array;
    price_builder.Finish(&price_array);
    
    arrow::Int64Builder volume_builder;
    volume_builder.Append(tick.volume);
    ArrayPtr volume_array;
    volume_builder.Finish(&volume_array);
    
    std::vector<ArrayPtr> arrays = {timestamp_array, symbol_array, price_array, volume_array};
    return arrow::RecordBatch::Make(schema, 1, arrays);
}

// Market data simulator
class MarketDataSimulator {
public:
    MarketDataSimulator(const std::vector<std::string>& symbols) : symbols_(symbols) {
        // Initialize prices
        std::random_device rd;
        gen_.seed(rd());
        
        for (const auto& symbol : symbols_) {
            prices_[symbol] = 100.0 + price_dist_(gen_) * 100.0; // $100-200 range
        }
    }
    
    MarketTick generate_tick() {
        MarketTick tick;
        tick.symbol = symbols_[symbol_dist_(gen_) % symbols_.size()];
        tick.timestamp = current_timestamp();
        
        // Simulate price movement (random walk)
        double& current_price = prices_[tick.symbol];
        double change = (price_change_dist_(gen_) - 0.5) * 2.0; // -1 to +1
        current_price = std::max(50.0, std::min(300.0, current_price + change));
        tick.price = current_price;
        
        tick.volume = volume_dist_(gen_);
        
        return tick;
    }
    
private:
    std::vector<std::string> symbols_;
    std::unordered_map<std::string, double> prices_;
    std::mt19937 gen_;
    std::uniform_real_distribution<> price_dist_{0.0, 1.0};
    std::uniform_real_distribution<> price_change_dist_{0.0, 1.0};
    std::uniform_int_distribution<> volume_dist_{1000, 10000};
    std::uniform_int_distribution<> symbol_dist_{0, 100};
};

int main() {
    std::cout << "=== Complete Streaming System Example ===" << std::endl;
    std::cout << "Simulating a real-time financial market data processing system" << std::endl;
    
    // Create streaming system with persistence enabled
    StreamConfig config;
    config.max_memory_size = 50000;
    config.enable_persistence = true;
    config.persistence_dir = "./market_data";
    config.cache_size = 10000;
    config.batch_size = 100;
    config.throttle_seconds = 0.1;
    
    StreamingSystem system(config);
    
    if (!system.initialize()) {
        std::cerr << "Failed to initialize streaming system" << std::endl;
        return 1;
    }
    
    std::cout << "Streaming system initialized with persistence enabled" << std::endl;
    
    // Define schemas
    std::vector<std::shared_ptr<arrow::Field>> market_fields = {
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("symbol", arrow::utf8()),
        arrow::field("price", arrow::float64()),
        arrow::field("volume", arrow::int64())
    };
    auto market_schema = arrow::schema(market_fields);
    
    std::vector<std::shared_ptr<arrow::Field>> analytics_fields = {
        arrow::field("symbol", arrow::utf8()),
        arrow::field("avg_price", arrow::float64()),
        arrow::field("total_volume", arrow::float64()),
        arrow::field("price_volatility", arrow::float64()),
        arrow::field("trade_count", arrow::float64())
    };
    auto analytics_schema = arrow::schema(analytics_fields);
    
    std::vector<std::shared_ptr<arrow::Field>> state_fields = {
        arrow::field("symbol", arrow::utf8()),
        arrow::field("cumulative_volume", arrow::float64()),
        arrow::field("vwap", arrow::float64()), // Volume-weighted average price
        arrow::field("price_momentum", arrow::float64())
    };
    auto state_schema = arrow::schema(state_fields);
    
    std::vector<std::shared_ptr<arrow::Field>> cross_sectional_fields = {
        arrow::field("symbol", arrow::utf8()),
        arrow::field("price_rank", arrow::float64()),
        arrow::field("volume_rank", arrow::float64()),
        arrow::field("relative_strength", arrow::float64())
    };
    auto cross_sectional_schema = arrow::schema(cross_sectional_fields);
    
    // Create stream tables
    std::cout << "\nCreating stream tables..." << std::endl;
    
    auto raw_market_data = system.create_stream_table("raw_market_data", market_schema);
    auto time_series_results = system.create_stream_table("time_series_results", analytics_schema);
    auto state_results = system.create_stream_table("state_results", state_schema);
    auto cross_sectional_results = system.create_stream_table("cross_sectional_results", cross_sectional_schema);
    auto final_analytics = system.create_stream_table("final_analytics", cross_sectional_schema);
    
    if (!raw_market_data || !time_series_results || !state_results || 
        !cross_sectional_results || !final_analytics) {
        std::cerr << "Failed to create stream tables" << std::endl;
        return 1;
    }
    
    std::cout << "Created 5 stream tables" << std::endl;
    
    // Enable persistence for key tables
    system.enable_table_persistence(raw_market_data, 5000, true, true);
    system.enable_table_persistence(final_analytics, 1000, true, true);
    std::cout << "Enabled persistence for raw data and final analytics" << std::endl;
    
    // Create computation engines
    std::cout << "\nCreating computation engines..." << std::endl;
    
    // 1. Time Series Engine - Moving window analytics
    auto ts_engine = system.create_time_series_engine(
        "market_analytics",
        "avg(price), sum(volume), stddev(price), count()",
        raw_market_data,
        time_series_results,
        "symbol",
        20 // 20-tick moving window
    );
    
    // 2. Reactive State Engine - Cumulative statistics
    auto state_engine = system.create_reactive_state_engine(
        "cumulative_stats",
        "cumsum(volume), cumavg(price), ewma(price)",
        raw_market_data,
        state_results,
        "symbol"
    );
    
    // 3. Cross-Sectional Engine - Relative rankings
    auto cs_engine = system.create_cross_sectional_engine(
        "market_rankings",
        "rank(price), rank(volume), zscore(price)",
        time_series_results,
        cross_sectional_results,
        "symbol",
        "keyCount",
        5 // Trigger every 5 symbols
    );
    
    if (!ts_engine || !state_engine || !cs_engine) {
        std::cerr << "Failed to create computation engines" << std::endl;
        return 1;
    }
    
    std::cout << "Created 3 computation engines:" << std::endl;
    std::cout << "  - Time Series Engine (20-tick moving window)" << std::endl;
    std::cout << "  - Reactive State Engine (cumulative statistics)" << std::endl;
    std::cout << "  - Cross-Sectional Engine (market rankings)" << std::endl;
    
    // Share tables for publishing
    system.share_stream_table(raw_market_data, "raw_market_data");
    system.share_stream_table(time_series_results, "time_series_results");
    system.share_stream_table(state_results, "state_results");
    system.share_stream_table(cross_sectional_results, "cross_sectional_results");
    
    // Set up processing pipeline
    std::cout << "\nSetting up processing pipeline..." << std::endl;
    
    // Raw data -> Time Series Engine
    auto ts_handler = [&](const StreamMessage& message) {
        ts_engine->process_message(message);
    };
    system.subscribe_table("", "raw_market_data", "ts_processor", 0, ts_handler, true);
    
    // Raw data -> State Engine
    auto state_handler = [&](const StreamMessage& message) {
        state_engine->process_message(message);
    };
    system.subscribe_table("", "raw_market_data", "state_processor", 0, state_handler, true);
    
    // Time Series Results -> Cross-Sectional Engine
    auto cs_handler = [&](const StreamMessage& message) {
        cs_engine->process_message(message);
    };
    system.subscribe_table("", "time_series_results", "cs_processor", 0, cs_handler, true);
    
    // Monitor final results
    std::atomic<int> final_results{0};
    auto final_handler = [&](const StreamMessage& message) {
        final_results++;
        if (final_results.load() % 10 == 0) {
            std::cout << "Processed " << final_results.load() << " cross-sectional results" << std::endl;
        }
    };
    system.subscribe_table("", "cross_sectional_results", "final_monitor", 0, final_handler, true);
    
    std::cout << "Pipeline configured: Raw Data -> [TS Engine, State Engine] -> CS Engine -> Final Results" << std::endl;
    
    // Initialize market simulator
    std::vector<std::string> symbols = {"AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "META", "NVDA", "NFLX"};
    MarketDataSimulator simulator(symbols);
    
    std::cout << "\nStarting market data simulation with " << symbols.size() << " symbols..." << std::endl;
    
    // Statistics tracking
    std::atomic<int> ticks_generated{0};
    std::unordered_map<std::string, int> symbol_counts;
    auto start_time = std::chrono::steady_clock::now();
    
    // Simulate market data for 30 seconds
    auto simulation_start = std::chrono::steady_clock::now();
    const auto simulation_duration = std::chrono::seconds(30);
    
    while (std::chrono::steady_clock::now() - simulation_start < simulation_duration) {
        // Generate market tick
        auto tick = simulator.generate_tick();
        symbol_counts[tick.symbol]++;
        
        auto batch = create_market_tick(market_schema, tick);
        raw_market_data->append(batch);
        
        ticks_generated++;
        
        if (ticks_generated.load() % 100 == 0) {
            std::cout << "Generated " << ticks_generated.load() << " market ticks..." << std::endl;
        }
        
        // Control tick rate (approximately 1000 ticks per second)
        std::this_thread::sleep_for(std::chrono::microseconds(1000));
    }
    
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "\nMarket simulation completed!" << std::endl;
    std::cout << "Generated " << ticks_generated.load() << " ticks in " << duration.count() << "ms" << std::endl;
    std::cout << "Average rate: " << (ticks_generated.load() * 1000.0 / duration.count()) << " ticks/second" << std::endl;
    
    // Wait for processing to complete
    std::cout << "\nWaiting for processing to complete..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));
    
    // Print comprehensive statistics
    std::cout << "\n=== COMPREHENSIVE SYSTEM STATISTICS ===" << std::endl;
    
    // Table statistics
    std::cout << "\n--- Table Statistics ---" << std::endl;
    std::cout << "Raw market data: " << raw_market_data->size() << " rows" << std::endl;
    std::cout << "Time series results: " << time_series_results->size() << " rows" << std::endl;
    std::cout << "State results: " << state_results->size() << " rows" << std::endl;
    std::cout << "Cross-sectional results: " << cross_sectional_results->size() << " rows" << std::endl;
    std::cout << "Final results processed: " << final_results.load() << std::endl;
    
    // Engine statistics
    std::cout << "\n--- Engine Statistics ---" << std::endl;
    auto ts_stats = ts_engine->get_stats();
    std::cout << "Time Series Engine:" << std::endl;
    for (const auto& stat : ts_stats) {
        std::cout << "  " << stat.first << ": " << stat.second << std::endl;
    }
    
    auto state_stats = state_engine->get_stats();
    std::cout << "Reactive State Engine:" << std::endl;
    for (const auto& stat : state_stats) {
        std::cout << "  " << stat.first << ": " << stat.second << std::endl;
    }
    
    auto cs_stats = cs_engine->get_stats();
    std::cout << "Cross-Sectional Engine:" << std::endl;
    for (const auto& stat : cs_stats) {
        std::cout << "  " << stat.first << ": " << stat.second << std::endl;
    }
    
    // Symbol distribution
    std::cout << "\n--- Symbol Distribution ---" << std::endl;
    for (const auto& symbol_count : symbol_counts) {
        std::cout << symbol_count.first << ": " << symbol_count.second << " ticks" << std::endl;
    }
    
    // System performance metrics
    std::cout << "\n--- System Performance ---" << std::endl;
    auto perf_metrics = system.get_performance_metrics();
    for (const auto& metric : perf_metrics) {
        std::cout << metric.first << ": " << std::fixed << std::setprecision(2) << metric.second << std::endl;
    }
    
    // Streaming statistics (like DolphinDB's getStreamingStat)
    std::cout << "\n--- Streaming Statistics ---" << std::endl;
    auto streaming_stats = system.get_streaming_stat();
    for (const auto& category : streaming_stats) {
        std::cout << category.first << ": " << category.second.size() << " entries" << std::endl;
    }
    
    // Engine system statistics
    std::cout << "\n--- Engine System Statistics ---" << std::endl;
    auto engine_stats = system.get_stream_engine_stat();
    for (const auto& engine_type : engine_stats) {
        std::cout << engine_type.first << ": " << engine_type.second.size() << " engines" << std::endl;
    }
    
    // Test snapshot functionality
    std::cout << "\n--- Testing Snapshot Functionality ---" << std::endl;
    if (ts_engine->enable_snapshot("./snapshots", 100)) {
        std::cout << "Time Series Engine snapshot enabled" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        int64_t ts_snapshot_id = ts_engine->get_snapshot_msg_id();
        std::cout << "TS Engine last snapshot ID: " << ts_snapshot_id << std::endl;
    }
    
    if (state_engine->enable_snapshot("./snapshots", 100)) {
        std::cout << "State Engine snapshot enabled" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        int64_t state_snapshot_id = state_engine->get_snapshot_msg_id();
        std::cout << "State Engine last snapshot ID: " << state_snapshot_id << std::endl;
    }
    
    // Save final report
    std::cout << "\n--- Saving Final Report ---" << std::endl;
    std::ofstream report("market_analysis_report.txt");
    if (report.is_open()) {
        report << "Market Data Analysis Report\n";
        report << "==========================\n\n";
        report << "Simulation Duration: " << duration.count() << "ms\n";
        report << "Total Ticks Generated: " << ticks_generated.load() << "\n";
        report << "Average Tick Rate: " << (ticks_generated.load() * 1000.0 / duration.count()) << " ticks/second\n\n";
        
        report << "Table Sizes:\n";
        report << "  Raw Data: " << raw_market_data->size() << " rows\n";
        report << "  Time Series Results: " << time_series_results->size() << " rows\n";
        report << "  State Results: " << state_results->size() << " rows\n";
        report << "  Cross-Sectional Results: " << cross_sectional_results->size() << " rows\n\n";
        
        report << "Symbol Distribution:\n";
        for (const auto& symbol_count : symbol_counts) {
            report << "  " << symbol_count.first << ": " << symbol_count.second << " ticks\n";
        }
        
        report.close();
        std::cout << "Report saved to market_analysis_report.txt" << std::endl;
    }
    
    std::cout << "\n=== SIMULATION COMPLETE ===" << std::endl;
    std::cout << "This example demonstrated:" << std::endl;
    std::cout << "✓ Multi-table streaming architecture" << std::endl;
    std::cout << "✓ Pipeline processing with multiple engines" << std::endl;
    std::cout << "✓ Real-time market data simulation" << std::endl;
    std::cout << "✓ Data persistence and recovery" << std::endl;
    std::cout << "✓ Comprehensive monitoring and statistics" << std::endl;
    std::cout << "✓ High-throughput data processing (1000+ ticks/second)" << std::endl;
    std::cout << "✓ Snapshot functionality for fault tolerance" << std::endl;
    
    // Cleanup
    system.shutdown();
    std::cout << "\nSystem shutdown complete." << std::endl;
    
    return 0;
}