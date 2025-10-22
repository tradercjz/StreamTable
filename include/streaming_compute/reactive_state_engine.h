#pragma once

#include "stream_engine.h"

namespace streaming_compute {

/**
 * ReactiveStateEngine - Performs stateful computations with incremental updates
 * 
 * Features:
 * - Incremental state updates
 * - Custom state functions
 * - Per-key state management
 * - Efficient memory usage
 */
class ReactiveStateEngine : public StreamEngine {
public:
    /**
     * Constructor
     * @param name Engine name
     * @param metrics Computation metrics (e.g., "cumsum(price), cumavg(volume)")
     * @param dummy_table Input table schema reference
     * @param output_table Output table for results
     * @param key_column Column name for partitioning
     */
    ReactiveStateEngine(const std::string& name,
                       const std::string& metrics,
                       std::shared_ptr<StreamTable> dummy_table,
                       std::shared_ptr<StreamTable> output_table,
                       const std::string& key_column = "");

    /**
     * Process a single message
     * @param message Input message to process
     * @return true if successful, false otherwise
     */
    bool process_message(const StreamMessage& message) override;

    /**
     * Get engine statistics
     * @return Map of statistics
     */
    std::unordered_map<std::string, int64_t> get_stats() const override;

    /**
     * Reset engine state
     */
    void reset() override;

    /**
     * Get state for a specific key
     * @param key Key to get state for
     * @return Map of state values
     */
    std::unordered_map<std::string, double> get_key_state(const std::string& key) const;

    /**
     * Set state for a specific key
     * @param key Key to set state for
     * @param state State values to set
     */
    void set_key_state(const std::string& key, const std::unordered_map<std::string, double>& state);

protected:
    /**
     * Serialize engine state
     * @param out Output stream
     * @return true if successful, false otherwise
     */
    bool serialize_state(std::ostream& out) const override;

    /**
     * Deserialize engine state
     * @param in Input stream
     * @return true if successful, false otherwise
     */
    bool deserialize_state(std::istream& in) override;

private:
    struct KeyState {
        std::unordered_map<std::string, double> values;
        std::unordered_map<std::string, int64_t> counts;
        int64_t total_messages = 0;
        std::chrono::system_clock::time_point last_update;
        
        KeyState() : last_update(std::chrono::system_clock::now()) {}
        
        void update_metric(const std::string& metric, double value);
        double get_metric_value(const std::string& metric) const;
    };

    std::string metrics_;
    std::vector<std::string> parsed_metrics_;
    
    // Per-key state data
    std::unordered_map<std::string, KeyState> key_states_;
    mutable std::shared_mutex states_mutex_;
    
    // Statistics
    std::atomic<int64_t> states_created_{0};
    std::atomic<int64_t> state_updates_{0};
    
    // Helper methods
    void parse_metrics();
    std::string extract_key(RecordBatchPtr batch) const;
    RecordBatchPtr create_output_batch(const std::string& key, const KeyState& state) const;
    void update_state(const std::string& key, RecordBatchPtr batch);
    double compute_cumsum(const std::string& column, RecordBatchPtr batch, KeyState& state);
    double compute_cumavg(const std::string& column, RecordBatchPtr batch, KeyState& state);
    double compute_cummax(const std::string& column, RecordBatchPtr batch, KeyState& state);
    double compute_cummin(const std::string& column, RecordBatchPtr batch, KeyState& state);
    double compute_cumcount(RecordBatchPtr batch, KeyState& state);
    double compute_cumprod(const std::string& column, RecordBatchPtr batch, KeyState& state);
    double compute_ewma(const std::string& column, RecordBatchPtr batch, KeyState& state, double alpha = 0.1);
    
    // Custom state functions
    double compute_custom_function(const std::string& function_name, 
                                  const std::string& column, 
                                  RecordBatchPtr batch, 
                                  KeyState& state);
};

} // namespace streaming_compute