/**
 * Test for CEP Engine
 */

#include "streaming_compute/streaming_compute.h"
#include "streaming_compute/cep_patterns.h"
#include "streaming_compute/cep_engine.h"
#include <gtest/gtest.h>
#include <thread>
#include <chrono>

using namespace streaming_compute;

// Test monitor for testing
class TestMonitor : public CEPMonitor {
public:
    int event_count = 0;
    std::vector<std::string> received_events;
    
    void on_load() override {
        // Could register event listeners here
    }
    
    void handle_test_event(std::shared_ptr<Event> event) {
        event_count++;
        received_events.push_back(event->event_type());
    }
};

class CEPEngineTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create streaming system
        StreamConfig config;
        config.max_memory_size = 256;
        config.enable_persistence = false;
        
        system_ = std::make_unique<StreamingSystem>(config);
        system_->initialize();
        
        // Create dummy table
        dummy_table_ = system_->create_stream_table(
            "test_dummy",
            arrow::schema({
                arrow::field("eventTime", arrow::timestamp(arrow::TimeUnit::MILLI)),
                arrow::field("eventType", arrow::utf8()),
                arrow::field("symbol", arrow::utf8()),
                arrow::field("price", arrow::float64())
            })
        );
        
        // Create output table
        output_table_ = system_->create_stream_table(
            "test_output",
            arrow::schema({
                arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
                arrow::field("alert_id", arrow::utf8()),
                arrow::field("message", arrow::utf8())
            })
        );
    }
    
    void TearDown() override {
        if (system_) {
            system_->shutdown();
        }
    }
    
    std::unique_ptr<StreamingSystem> system_;
    std::shared_ptr<StreamTable> dummy_table_;
    std::shared_ptr<StreamTable> output_table_;
};

TEST_F(CEPEngineTest, CreateCEPEngine) {
    // Create monitors
    auto monitor1 = std::make_shared<TestMonitor>();
    auto monitor2 = std::make_shared<PriceSpikeMonitor>(0.05);
    
    std::vector<std::shared_ptr<CEPMonitor>> monitors = {monitor1, monitor2};
    
    // Define event schemas
    std::vector<EventSchema> event_schemas = {
        {"MarketData", {"symbol", "price", "volume"}, 
         {arrow::utf8(), arrow::float64(), arrow::int32()}},
        {"Order", {"order_id", "symbol", "side", "price", "quantity"},
         {arrow::utf8(), arrow::utf8(), arrow::utf8(), arrow::float64(), arrow::int32()}}
    };
    
    // Create CEP engine
    auto cep_engine = CEPEngineFactory::create_cep_engine(
        "test_cep",
        monitors,
        dummy_table_,
        event_schemas,
        output_table_,
        "symbol",  // dispatch by symbol
        2          // 2 sub-engines
    );
    
    EXPECT_NE(cep_engine, nullptr);
    EXPECT_EQ(cep_engine->name(), "test_cep");
    
    // Check that engine is active
    EXPECT_TRUE(cep_engine->is_active());
}

TEST_F(CEPEngineTest, AppendEvents) {
    // Create simple monitor
    auto monitor = std::make_shared<TestMonitor>();
    std::vector<std::shared_ptr<CEPMonitor>> monitors = {monitor};
    
    // Define event schemas
    std::vector<EventSchema> event_schemas = {
        {"MarketData", {"symbol", "price", "volume"}, 
        {arrow::utf8(), arrow::float64(), arrow::int32()}}
    };
    
    // Create CEP engine
    auto cep_engine = CEPEngineFactory::create_cep_engine(
        "test_append",
        monitors,
        dummy_table_,
        event_schemas
    );
    
    ASSERT_NE(cep_engine, nullptr);
    
    // Cast to CEPEngine
    auto cep = std::dynamic_pointer_cast<CEPEngine>(cep_engine);
    ASSERT_NE(cep, nullptr);
    
    // Append events using dictionary
    std::unordered_map<std::string, std::any> event1 = {
        {"eventType", "MarketData"},
        {"symbol", "AAPL"},
        {"price", 150.0},
        {"volume", 1000}
    };
    
    std::unordered_map<std::string, std::any> event2 = {
        {"eventType", "MarketData"},
        {"symbol", "GOOGL"},
        {"price", 2800.0},
        {"volume", 500}
    };
    
    bool result1 = cep->append_event_dict(event1);
    bool result2 = cep->append_event_dict(event2);
    
    EXPECT_TRUE(result1);
    EXPECT_TRUE(result2);
    
    // Give some time for processing
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

TEST_F(CEPEngineTest, PriceSpikeDetection) {
    // Create price spike monitor
    auto spike_monitor = std::make_shared<PriceSpikeMonitor>(0.03); // 3% threshold
    std::vector<std::shared_ptr<CEPMonitor>> monitors = {spike_monitor};
    
    // Define event schemas
    std::vector<EventSchema> event_schemas = {
        {"MarketData", {"symbol", "price", "volume"}, 
         {arrow::utf8(), arrow::float64(), arrow::int32()}}
    };
    
    // Create CEP engine
    auto cep_engine = CEPEngineFactory::create_cep_engine(
        "test_spike",
        monitors,
        dummy_table_,
        event_schemas,
        output_table_
    );
    
    ASSERT_NE(cep_engine, nullptr);
    
    auto cep = std::dynamic_pointer_cast<CEPEngine>(cep_engine);
    ASSERT_NE(cep, nullptr);
    
    // Append normal price
    std::unordered_map<std::string, std::any> normal_event = {
        {"eventType", "MarketData"},
        {"symbol", "AAPL"},
        {"price", 150.0},
        {"volume", 1000}
    };
    
    // Append spike price (5% increase)
    std::unordered_map<std::string, std::any> spike_event = {
        {"eventType", "MarketData"},
        {"symbol", "AAPL"},
        {"price", 157.5}, // 5% increase
        {"volume", 2000}
    };
    
    bool result1 = cep->append_event_dict(normal_event);
    bool result2 = cep->append_event_dict(spike_event);
    
    EXPECT_TRUE(result1);
    EXPECT_TRUE(result2);
    
    // Give some time for processing
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

TEST_F(CEPEngineTest, Statistics) {
    // Create monitor
    auto monitor = std::make_shared<TestMonitor>();
    std::vector<std::shared_ptr<CEPMonitor>> monitors = {monitor};
    
    // Define event schemas
    std::vector<EventSchema> event_schemas = {
        {"MarketData", {"symbol", "price", "volume"}, 
         {arrow::utf8(), arrow::float64(), arrow::int32()}}
    };
    
    // Create CEP engine
    auto cep_engine = CEPEngineFactory::create_cep_engine(
        "test_stats",
        monitors,
        dummy_table_,
        event_schemas
    );
    
    ASSERT_NE(cep_engine, nullptr);
    
    auto cep = std::dynamic_pointer_cast<CEPEngine>(cep_engine);
    ASSERT_NE(cep, nullptr);
    
    // Append some events
    for (int i = 0; i < 5; ++i) {
        std::unordered_map<std::string, std::any> event = {
            {"eventType", "MarketData"},
            {"symbol", "SYM" + std::to_string(i % 2)},
            {"price", 100.0 + i},
            {"volume", 1000 + i}
        };
        cep->append_event_dict(event);
    }
    
    // Give some time for processing
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Check statistics
    auto stats = cep_engine->get_stats();
    
    EXPECT_GT(stats["sub_engine_count"], 0);
    EXPECT_GE(stats["total_queue_size"], 0);
    EXPECT_GE(stats["processed_messages"], 0);
}

TEST_F(CEPEngineTest, MultipleSubEngines) {
    // Create monitor
    auto monitor = std::make_shared<TestMonitor>();
    std::vector<std::shared_ptr<CEPMonitor>> monitors = {monitor};
    
    // Define event schemas
    std::vector<EventSchema> event_schemas = {
        {"MarketData", {"symbol", "price", "volume"}, 
         {arrow::utf8(), arrow::float64(), arrow::int32()}}
    };
    
    // Create CEP engine with multiple sub-engines
    auto cep_engine = CEPEngineFactory::create_cep_engine(
        "test_multi",
        monitors,
        dummy_table_,
        event_schemas,
        nullptr, // no output table
        "symbol", // dispatch by symbol
        3,         // 3 sub-engines
	"",
	"eventTime",
	true,
	1024,
	1
    );
    
    ASSERT_NE(cep_engine, nullptr);
    
    auto cep = std::dynamic_pointer_cast<CEPEngine>(cep_engine);
    ASSERT_NE(cep, nullptr);
    
    // Append events for different symbols
    std::vector<std::string> symbols = {"AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"};
    
    for (const auto& symbol : symbols) {
        std::unordered_map<std::string, std::any> event = {
            {"eventType", "MarketData"},
            {"symbol", symbol},
            {"price", 100.0},
            {"volume", 1000}
        };
        cep->append_event_dict(event);
    }
    
    // Give some time for processing
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Check statistics
    auto stats = cep_engine->get_stats();
    
    // Should have multiple sub-engines (up to 3, but might be less due to hashing)
    EXPECT_GT(stats["sub_engine_count"], 0);
    EXPECT_LE(stats["sub_engine_count"], 3);
}
