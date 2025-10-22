/**
 * CEP Engine Example
 * 
 * This example demonstrates how to use the CEP engine for complex event processing.
 * It creates a CEP engine with multiple monitors to detect various patterns in
 * market data and order events.
 */

#include "streaming_compute/streaming_compute.h"
#include "streaming_compute/cep_patterns.h"
#include "streaming_compute/cep_engine.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <random>

using namespace streaming_compute;

// Function to generate random market data
std::shared_ptr<MarketDataEvent> generate_market_data(const std::string& symbol, double base_price) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_real_distribution<> price_var(-0.01, 0.01);
    static std::uniform_int_distribution<> volume_dist(100, 10000);
    
    double price = base_price * (1.0 + price_var(gen));
    int volume = volume_dist(gen);
    
    return std::make_shared<MarketDataEvent>(symbol, price, volume);
}

// Function to generate random orders
std::shared_ptr<OrderEvent> generate_order(const std::string& symbol, double current_price) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> side_dist(0, 1);
    static std::uniform_int_distribution<> qty_dist(100, 5000);
    static std::uniform_real_distribution<> price_var(-0.02, 0.02);
    
    std::string side = side_dist(gen) == 0 ? "BUY" : "SELL";
    int quantity = qty_dist(gen);
    double price = current_price * (1.0 + price_var(gen));
    
    static int order_id_counter = 1;
    std::string order_id = "ORD" + std::to_string(order_id_counter++);
    
    return std::make_shared<OrderEvent>(order_id, symbol, side, price, quantity);
}

int main() {
    std::cout << "=== CEP Engine Example ===" << std::endl;
    
    try {
        // Create streaming system
        StreamConfig config;
        config.max_memory_size = 1024;
        config.enable_persistence = false;
        
        StreamingSystem system(config);
        
        if (!system.initialize()) {
            std::cerr << "Failed to initialize streaming system" << std::endl;
            return 1;
        }
        
        std::cout << "Streaming system initialized successfully" << std::endl;
        
        // Create stream tables
        auto dummy_table = system.create_stream_table(
            "dummy_table",
            arrow::schema({
                arrow::field("eventTime", arrow::timestamp(arrow::TimeUnit::MILLI)),
                arrow::field("eventType", arrow::utf8()),
                arrow::field("symbol", arrow::utf8()),
                arrow::field("price", arrow::float64()),
                arrow::field("volume", arrow::int32())
            })
        );
        
        auto output_table = system.create_stream_table(
            "alert_table",
            arrow::schema({
                arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
                arrow::field("alert_id", arrow::utf8()),
                arrow::field("alert_type", arrow::utf8()),
                arrow::field("message", arrow::utf8()),
                arrow::field("severity", arrow::utf8())
            })
        );
        
        // Create monitors
        auto price_spike_monitor = std::make_shared<PriceSpikeMonitor>(0.03); // 3% threshold
        auto volume_monitor = std::make_shared<VolumeMonitor>(30000, 50000); // 30s window, 50k threshold
        auto pattern_monitor = std::make_shared<PatternMonitor>();
        
        std::vector<std::shared_ptr<CEPMonitor>> monitors = {
            price_spike_monitor,
            volume_monitor,
            pattern_monitor
        };
        
        // Define event schemas
        std::vector<EventSchema> event_schemas = {
            {"MarketData", {"symbol", "price", "volume", "market"}, 
             {arrow::utf8(), arrow::float64(), arrow::int32(), arrow::utf8()}},
            {"Order", {"order_id", "symbol", "side", "price", "quantity", "order_type"},
             {arrow::utf8(), arrow::utf8(), arrow::utf8(), arrow::float64(), arrow::int32(), arrow::utf8()}},
            {"Alert", {"alert_id", "alert_type", "message", "severity"},
             {arrow::utf8(), arrow::utf8(), arrow::utf8(), arrow::utf8()}}
        };
        
        // Create CEP engine
        auto cep_engine = CEPEngineFactory::create_cep_engine(
            "market_cep",
            monitors,
            dummy_table,
            event_schemas,
            output_table,
            "symbol",  // dispatch by symbol
            4,         // 4 sub-engines
            "eventTime",
            "eventTime",
            true,      // use system time
            2048,      // event queue depth
            2          // deserialization parallelism
        );
        
        if (!cep_engine) {
            std::cerr << "Failed to create CEP engine" << std::endl;
            return 1;
        }
        
        std::cout << "CEP engine created successfully" << std::endl;
        std::cout << "Starting event simulation..." << std::endl;
        
        // Simulate events
        std::vector<std::string> symbols = {"AAPL", "GOOGL", "MSFT", "AMZN"};
        std::unordered_map<std::string, double> base_prices = {
            {"AAPL", 150.0},
            {"GOOGL", 2800.0},
            {"MSFT", 300.0},
            {"AMZN", 3300.0}
        };
        
        int event_count = 0;
        const int total_events = 1000;
        
        for (int i = 0; i < total_events; ++i) {
            // Randomly select a symbol
            static std::random_device rd;
            static std::mt19937 gen(rd());
            static std::uniform_int_distribution<> symbol_dist(0, symbols.size() - 1);
            static std::uniform_int_distribution<> event_type_dist(0, 1);
            
            std::string symbol = symbols[symbol_dist(gen)];
            double base_price = base_prices[symbol];
            
            // Generate market data or order event
            if (event_type_dist(gen) == 0) {
                // Market data event
                auto market_event = generate_market_data(symbol, base_price);
                
                // Occasionally create a price spike
                if (i % 50 == 0) {
                    market_event = std::make_shared<MarketDataEvent>(
                        symbol, base_price * 1.05, 1000); // 5% spike
                }
                
                // Convert to dictionary format for append
                std::unordered_map<std::string, std::any> event_dict = {
                    {"eventType", "MarketData"},
                    {"symbol", symbol},
                    {"price", market_event->price},
                    {"volume", market_event->volume},
                    {"market", "NASDAQ"}
                };
                
                if (auto cep = std::dynamic_pointer_cast<CEPEngine>(cep_engine)) {
                    cep->append_event_dict(event_dict);
                }
                
                std::cout << "Generated MarketData: " << market_event->to_string() << std::endl;
            } else {
                // Order event
                auto order_event = generate_order(symbol, base_price);
                
                // Occasionally create a large buy order
                if (i % 30 == 0) {
                    order_event = std::make_shared<OrderEvent>(
                        "LARGE_" + std::to_string(i), symbol, "BUY", base_price, 20000);
                }
                
                std::unordered_map<std::string, std::any> event_dict = {
                    {"eventType", "Order"},
                    {"order_id", order_event->order_id},
                    {"symbol", symbol},
                    {"side", order_event->side},
                    {"price", order_event->price},
                    {"quantity", order_event->quantity},
                    {"order_type", "LIMIT"}
                };
                
                if (auto cep = std::dynamic_pointer_cast<CEPEngine>(cep_engine)) {
                    cep->append_event_dict(event_dict);
                }
                
                std::cout << "Generated Order: " << order_event->to_string() << std::endl;
            }
            
            event_count++;
            
            // Sleep to simulate real-time events
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            // Print statistics every 100 events
            if (event_count % 100 == 0) {
                auto stats = cep_engine->get_stats();
                std::cout << "\n=== Statistics (Event " << event_count << ") ===" << std::endl;
                std::cout << "Processed messages: " << stats["processed_messages"] << std::endl;
                std::cout << "Sub-engine count: " << stats["sub_engine_count"] << std::endl;
                std::cout << "Total queue size: " << stats["total_queue_size"] << std::endl;
                std::cout << "========================\n" << std::endl;
            }
        }
        
        std::cout << "\n=== Simulation Complete ===" << std::endl;
        std::cout << "Total events generated: " << event_count << std::endl;
        
        // Print final statistics
        auto final_stats = cep_engine->get_stats();
        std::cout << "\n=== Final Statistics ===" << std::endl;
        for (const auto& [key, value] : final_stats) {
            std::cout << key << ": " << value << std::endl;
        }
        
        // Shutdown system
        system.shutdown();
        std::cout << "\nCEP example completed successfully!" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
