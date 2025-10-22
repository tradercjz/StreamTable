#pragma once

#include "cep_engine.h"
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace streaming_compute {

// ============================================================================
// Concrete Event Classes
// ============================================================================

/**
 * MarketData event - represents stock market data
 */
class MarketDataEvent : public Event {
public:
    std::string symbol;
    double price;
    int volume;
    std::string market;
    
    MarketDataEvent(const std::string& sym, double p, int v, const std::string& mkt = "")
        : symbol(sym), price(p), volume(v), market(mkt) {
        set_system_time(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    }
    
    std::string event_type() const override { return "MarketData"; }
    
    std::string to_string() const override {
        return "MarketData{symbol=" + symbol + ", price=" + std::to_string(price) + 
               ", volume=" + std::to_string(volume) + "}";
    }
};

/**
 * Order event - represents trading orders
 */
class OrderEvent : public Event {
public:
    std::string order_id;
    std::string symbol;
    std::string side; // "BUY" or "SELL"
    double price;
    int quantity;
    std::string order_type;
    
    OrderEvent(const std::string& id, const std::string& sym, const std::string& s, 
               double p, int qty, const std::string& type = "LIMIT")
        : order_id(id), symbol(sym), side(s), price(p), quantity(qty), order_type(type) {
        set_system_time(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    }
    
    std::string event_type() const override { return "Order"; }
    
    std::string to_string() const override {
        return "Order{id=" + order_id + ", symbol=" + symbol + ", side=" + side + 
               ", price=" + std::to_string(price) + ", qty=" + std::to_string(quantity) + "}";
    }
};

/**
 * Alert event - represents system alerts
 */
class AlertEvent : public Event {
public:
    std::string alert_id;
    std::string alert_type;
    std::string message;
    std::string severity; // "INFO", "WARNING", "ERROR"
    
    AlertEvent(const std::string& id, const std::string& type, 
               const std::string& msg, const std::string& sev = "INFO")
        : alert_id(id), alert_type(type), message(msg), severity(sev) {
        set_system_time(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    }
    
    std::string event_type() const override { return "Alert"; }
    
    std::string to_string() const override {
        return "Alert{id=" + alert_id + ", type=" + alert_type + 
               ", severity=" + severity + ", message=" + message + "}";
    }
};

// ============================================================================
// Example Monitor Classes
// ============================================================================

/**
 * PriceSpikeMonitor - detects rapid price movements
 */
class PriceSpikeMonitor : public CEPMonitor {
private:
    std::unordered_map<std::string, double> last_prices_;
    std::unordered_map<std::string, int64_t> last_times_;
    double spike_threshold_;
    
public:
    PriceSpikeMonitor(double threshold = 0.05) : spike_threshold_(threshold) {}
    
    void on_load() override {
        // Register event listeners
        // add_event_listener<MarketDataEvent>(&PriceSpikeMonitor::handle_market_data, this);
    }
    
    void handle_market_data(std::shared_ptr<MarketDataEvent> event) {
        auto symbol = event->symbol;
        auto current_price = event->price;
        auto current_time = event->system_time();
        
        if (last_prices_.find(symbol) != last_prices_.end()) {
            double last_price = last_prices_[symbol];
            double price_change = std::abs(current_price - last_price) / last_price;
            
            if (price_change > spike_threshold_) {
                // Create alert for price spike
                auto alert = std::make_shared<AlertEvent>(
                    "price_spike_" + symbol,
                    "PRICE_SPIKE",
                    "Price spike detected for " + symbol + ": " + 
                    std::to_string(price_change * 100) + "% change",
                    "WARNING"
                );
                
                emit_event(alert);
            }
        }
        
        // Update last price and time
        last_prices_[symbol] = current_price;
        last_times_[symbol] = current_time;
    }
};

/**
 * VolumeMonitor - monitors trading volume
 */
class VolumeMonitor : public CEPMonitor {
private:
    std::unordered_map<std::string, int> volume_accumulators_;
    std::unordered_map<std::string, int64_t> window_start_times_;
    int64_t window_duration_ms_;
    int volume_threshold_;
    
public:
    VolumeMonitor(int64_t window_ms = 60000, int threshold = 1000000) 
        : window_duration_ms_(window_ms), volume_threshold_(threshold) {}
    
    void on_load() override {
        // Register event listeners
        // add_event_listener<MarketDataEvent>(&VolumeMonitor::handle_market_data, this);
    }
    
    void handle_market_data(std::shared_ptr<MarketDataEvent> event) {
        auto symbol = event->symbol;
        auto current_time = event->system_time();
        
        // Initialize window if needed
        if (window_start_times_.find(symbol) == window_start_times_.end()) {
            window_start_times_[symbol] = current_time;
            volume_accumulators_[symbol] = 0;
        }
        
        // Check if window has expired
        if (current_time - window_start_times_[symbol] > window_duration_ms_) {
            // Reset window
            window_start_times_[symbol] = current_time;
            volume_accumulators_[symbol] = 0;
        }
        
        // Accumulate volume
        volume_accumulators_[symbol] += event->volume;
        
        // Check threshold
        if (volume_accumulators_[symbol] > volume_threshold_) {
            auto alert = std::make_shared<AlertEvent>(
                "high_volume_" + symbol,
                "HIGH_VOLUME",
                "High volume detected for " + symbol + ": " + 
                std::to_string(volume_accumulators_[symbol]) + " shares",
                "INFO"
            );
            
            emit_event(alert);
            
            // Reset accumulator after alert
            volume_accumulators_[symbol] = 0;
        }
    }
};

/**
 * PatternMonitor - detects complex patterns across multiple events
 */
class PatternMonitor : public CEPMonitor {
private:
    struct PatternState {
        std::string symbol;
        int64_t pattern_start_time;
        std::vector<std::shared_ptr<Event>> events;
        bool pattern_matched;
    };
    
    std::unordered_map<std::string, PatternState> pattern_states_;
    
public:
    void on_load() override {
        // Register multiple event listeners
        // add_event_listener<MarketDataEvent>(&PatternMonitor::handle_market_data, this);
        // add_event_listener<OrderEvent>(&PatternMonitor::handle_order, this);
    }
    
    void handle_market_data(std::shared_ptr<MarketDataEvent> event) {
        auto symbol = event->symbol;
        
        // Initialize pattern state if needed
        if (pattern_states_.find(symbol) == pattern_states_.end()) {
            pattern_states_[symbol] = {symbol, event->system_time(), {}, false};
        }
        
        auto& state = pattern_states_[symbol];
        state.events.push_back(event);
        
        // Check for pattern: price drop followed by large buy order
        check_price_drop_buy_pattern(state);
        
        // Cleanup old events (keep only last 10 events)
        if (state.events.size() > 10) {
            state.events.erase(state.events.begin(), state.events.begin() + (state.events.size() - 10));
        }
    }
    
    void handle_order(std::shared_ptr<OrderEvent> event) {
        auto symbol = event->symbol;
        
        if (pattern_states_.find(symbol) != pattern_states_.end()) {
            auto& state = pattern_states_[symbol];
            state.events.push_back(event);
            
            // Check for pattern
            check_price_drop_buy_pattern(state);
        }
    }
    
private:
    void check_price_drop_buy_pattern(PatternState& state) {
        if (state.pattern_matched) return;
        
        // Look for pattern: price drop > 2% followed by large buy order
        double max_price = 0.0;
        double min_price = std::numeric_limits<double>::max();
        std::shared_ptr<OrderEvent> large_buy_order;
        
        for (const auto& event : state.events) {
            if (auto market_data = std::dynamic_pointer_cast<MarketDataEvent>(event)) {
                max_price = std::max(max_price, market_data->price);
                min_price = std::min(min_price, market_data->price);
            } else if (auto order = std::dynamic_pointer_cast<OrderEvent>(event)) {
                if (order->side == "BUY" && order->quantity > 10000) {
                    large_buy_order = order;
                }
            }
        }
        
        if (max_price > 0 && min_price < std::numeric_limits<double>::max() && large_buy_order) {
            double price_drop = (max_price - min_price) / max_price;
            
            if (price_drop > 0.02) {
                // Pattern matched!
                auto alert = std::make_shared<AlertEvent>(
                    "pattern_" + state.symbol,
                    "PRICE_DROP_BUY_PATTERN",
                    "Price drop followed by large buy order detected for " + state.symbol,
                    "WARNING"
                );
                
                emit_event(alert);
                state.pattern_matched = true;
            }
        }
    }
};

// ============================================================================
// CEP Pattern Examples
// ============================================================================

/**
 * Example CEP patterns that can be implemented
 */
class CEPPatterns {
public:
    /**
     * Sequence pattern: A followed by B followed by C
     */
    static bool sequence_pattern(const std::vector<std::shared_ptr<Event>>& events,
                                const std::vector<std::string>& event_types);
    
    /**
     * Time window pattern: Events occurring within a time window
     */
    static bool time_window_pattern(const std::vector<std::shared_ptr<Event>>& events,
                                   int64_t window_ms);
    
    /**
     * Threshold pattern: Value exceeds threshold
     */
    static bool threshold_pattern(double value, double threshold);
    
    /**
     * Trend pattern: Consistent upward or downward movement
     */
    static bool trend_pattern(const std::vector<double>& values, 
                             double min_trend_strength = 0.01);
    
    /**
     * Correlation pattern: Events correlated across different symbols
     */
    static bool correlation_pattern(const std::vector<std::shared_ptr<MarketDataEvent>>& events1,
                                   const std::vector<std::shared_ptr<MarketDataEvent>>& events2,
                                   double min_correlation = 0.8);
};

} // namespace streaming_compute