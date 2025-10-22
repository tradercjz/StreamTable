# CEP Engine Guide

## Overview

The Complex Event Processing (CEP) engine is a powerful component of the StreamTable library that enables real-time detection of complex patterns across multiple events. It is designed to handle high-throughput event streams and detect sophisticated patterns that span multiple events and time windows.

## Key Features

- **Event-driven processing**: Process complex events that can be combinations of multiple simple events
- **Pattern matching**: Define and detect specific patterns across multiple events
- **Monitor classes**: User-defined event handlers with lifecycle management
- **Event routing**: Flexible event routing to input queues, output queues, or pattern matching
- **Sub-engine architecture**: Parallel processing with multiple sub-engines based on dispatch keys
- **Time handling**: Support for both system time and event time
- **Thread-safe**: Designed for concurrent event processing

## Architecture

### Core Components

1. **CEP Engine**: Main engine that manages sub-engines and event distribution
2. **Sub-Engine**: Individual processing units that handle events for specific keys
3. **Monitor**: User-defined event handlers with event listener registration
4. **Event**: Base class for all events with time handling capabilities
5. **Event Schema**: Definition of event types and their fields

### Event Flow

```
Input Events → CEP Engine → Sub-Engines → Monitors → Output Events
                    │              │           │
                    │              │           └── Event Listeners
                    │              └── Event Queues
                    └── Dispatch Key Routing
```

## Quick Start

### 1. Define Event Classes

```cpp
#include "streaming_compute/cep_patterns.h"

// Custom event class
class MyCustomEvent : public streaming_compute::Event {
public:
    std::string symbol;
    double value;
    int count;
    
    MyCustomEvent(const std::string& sym, double val, int cnt)
        : symbol(sym), value(val), count(cnt) {
        set_system_time(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    }
    
    std::string event_type() const override { return "MyCustomEvent"; }
    
    std::string to_string() const override {
        return "MyCustomEvent{symbol=" + symbol + ", value=" + std::to_string(value) + "}";
    }
};
```

### 2. Create Monitor Classes

```cpp
class MyMonitor : public streaming_compute::CEPMonitor {
private:
    std::unordered_map<std::string, double> last_values_;
    
public:
    void on_load() override {
        // Register event listeners here
        // add_event_listener<MyCustomEvent>(&MyMonitor::handle_custom_event, this);
    }
    
    void handle_custom_event(std::shared_ptr<MyCustomEvent> event) {
        // Process the event
        std::string symbol = event->symbol;
        double current_value = event->value;
        
        // Example: Detect value spikes
        if (last_values_.find(symbol) != last_values_.end()) {
            double last_value = last_values_[symbol];
            double change = std::abs(current_value - last_value) / last_value;
            
            if (change > 0.1) { // 10% change
                // Create alert
                auto alert = std::make_shared<streaming_compute::AlertEvent>(
                    "spike_" + symbol,
                    "VALUE_SPIKE",
                    "Value spike detected for " + symbol,
                    "WARNING"
                );
                
                emit_event(alert);
            }
        }
        
        last_values_[symbol] = current_value;
    }
};
```

### 3. Create and Use CEP Engine

```cpp
#include "streaming_compute/streaming_compute.h"

// Create streaming system
streaming_compute::StreamConfig config;
streaming_compute::StreamingSystem system(config);
system.initialize();

// Create stream tables
auto dummy_table = system.create_stream_table(
    "dummy_table",
    arrow::schema({
        arrow::field("eventTime", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("eventType", arrow::utf8()),
        arrow::field("symbol", arrow::utf8()),
        arrow::field("value", arrow::float64())
    })
);

auto output_table = system.create_stream_table(
    "alert_table",
    arrow::schema({
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("alert_id", arrow::utf8()),
        arrow::field("alert_type", arrow::utf8()),
        arrow::field("message", arrow::utf8())
    })
);

// Create monitors
auto my_monitor = std::make_shared<MyMonitor>();
std::vector<std::shared_ptr<streaming_compute::CEPMonitor>> monitors = {my_monitor};

// Define event schemas
std::vector<streaming_compute::EventSchema> event_schemas = {
    {"MyCustomEvent", {"symbol", "value", "count"}, 
     {arrow::utf8(), arrow::float64(), arrow::int32()}}
};

// Create CEP engine
auto cep_engine = system.create_cep_engine(
    "my_cep_engine",
    monitors,
    dummy_table,
    event_schemas,
    output_table,
    "symbol",  // dispatch by symbol
    4,         // 4 sub-engines
    "eventTime",
    "eventTime",
    true,      // use system time
    1024,      // event queue depth
    2          // deserialization parallelism
);

// Append events
std::unordered_map<std::string, std::any> event_dict = {
    {"eventType", "MyCustomEvent"},
    {"symbol", "AAPL"},
    {"value", 150.0},
    {"count", 100}
};

if (auto cep = std::dynamic_pointer_cast<streaming_compute::CEPEngine>(cep_engine)) {
    cep->append_event_dict(event_dict);
}
```

## Built-in Patterns

The CEP engine comes with several built-in pattern monitors:

### PriceSpikeMonitor
Detects rapid price movements exceeding a specified threshold.

```cpp
auto spike_monitor = std::make_shared<streaming_compute::PriceSpikeMonitor>(0.05); // 5% threshold
```

### VolumeMonitor
Monitors trading volume within time windows.

```cpp
auto volume_monitor = std::make_shared<streaming_compute::VolumeMonitor>(
    60000,  // 60-second window
    100000  // 100,000 volume threshold
);
```

### PatternMonitor
Detects complex patterns across multiple event types.

```cpp
auto pattern_monitor = std::make_shared<streaming_compute::PatternMonitor>();
```

## Event Routing

Monitors can route events in different ways:

- **send_event(event)**: Insert event at the end of the input queue
- **route_event(event)**: Insert event at the beginning of the input queue
- **emit_event(event)**: Send event to output queue for further processing

## Configuration Options

### CEP Engine Parameters

- **name**: Engine identifier
- **monitors**: List of monitor instances
- **dummy_table**: Input table schema reference
- **event_schemas**: Definition of supported event types
- **output_table**: Output table for results (optional)
- **dispatch_key**: Field name for sub-engine distribution
- **dispatch_bucket**: Number of sub-engines
- **time_column**: Time column name
- **event_time_field**: Event time field name
- **use_system_time**: Use system time vs event time
- **event_queue_depth**: Maximum queue depth per sub-engine
- **deserialize_parallelism**: Number of deserialization threads

## Performance Considerations

1. **Queue Depth**: Set appropriate queue depth based on expected event rate
2. **Sub-Engine Count**: Balance between parallelism and resource usage
3. **Event Schema**: Define only necessary fields to reduce memory usage
4. **Monitor Complexity**: Keep monitor logic efficient for high-throughput scenarios

## Example Use Cases

### Financial Trading
- Detect arbitrage opportunities
- Monitor for market manipulation patterns
- Implement algorithmic trading strategies

### IoT Monitoring
- Detect equipment failure patterns
- Monitor for security breaches
- Implement predictive maintenance

### Network Security
- Detect DDoS attack patterns
- Monitor for intrusion attempts
- Implement real-time threat detection

## Best Practices

1. **Monitor Design**: Keep monitors focused on specific patterns
2. **Error Handling**: Implement robust error handling in monitor logic
3. **Resource Management**: Monitor memory usage and queue depths
4. **Testing**: Test patterns with realistic event streams
5. **Monitoring**: Use built-in statistics to monitor engine performance

## Troubleshooting

### Common Issues

1. **Queue Full**: Increase event_queue_depth or optimize monitor processing
2. **Memory Usage**: Monitor memory consumption and adjust configuration
3. **Pattern Not Detected**: Verify event timing and pattern logic
4. **Performance Issues**: Check sub-engine distribution and monitor complexity

### Debugging Tips

1. Use the built-in statistics to monitor engine performance
2. Implement logging in monitor classes
3. Test with simplified event streams first
4. Use the example code as a reference implementation