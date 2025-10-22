#pragma once

#include "stream_engine.h"
#include <deque>

namespace streaming_compute {

/**
 * TimeSeriesEngine - Performs time-based window computations
 * 
 * Features:
 * - Sliding window operations
 * - Time-based aggregations
 * - Multiple metrics support
 * - Configurable window sizes
 */
class TimeSeriesEngine : public StreamEngine {
public:
    /**
     * Constructor
     * @param name Engine name
     * @param metrics Computation metrics (e.g., "sum(price), avg(volume)")
     * @param dummy_table Input table schema reference
     * @param output_table Output table for results
     * @param key_column Column name for partitioning
     * @param window_size Number of records in sliding window
     */
    TimeSeriesEngine(const std::string& name,
                    const std::string& metrics,
                    std::shared_ptr<StreamTable> dummy_table,
                    std::shared_ptr<StreamTable> output_table,
                    const std::string& key_column = "",
                    size_t window_size = 100);

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
     * Set window size
     * @param window_size New window size
     */
    void set_window_size(size_t window_size);

    /**
     * Get current window size
     * @return Window size
     */
    size_t get_window_size() const { return window_size_; }

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
    struct WindowData {
        std::deque<RecordBatchPtr> batches;
        std::unordered_map<std::string, double> aggregated_values;
        int64_t total_rows = 0;
        
        void add_batch(RecordBatchPtr batch);
        void remove_old_batches(size_t max_size);
        void compute_aggregations(const std::vector<std::string>& metrics);
    };

    std::string metrics_;
    size_t window_size_;
    std::vector<std::string> parsed_metrics_;
    
    // Per-key window data
    std::unordered_map<std::string, WindowData> key_windows_;
    mutable std::shared_mutex windows_mutex_;
    
    // Statistics
    std::atomic<int64_t> windows_created_{0};
    std::atomic<int64_t> aggregations_computed_{0};
    
    // Helper methods
    void parse_metrics();
    std::string extract_key(RecordBatchPtr batch) const;
    RecordBatchPtr create_output_batch(const std::string& key, const WindowData& window_data) const;
    double compute_metric(const std::string& metric, const WindowData& window_data) const;
    double compute_sum(const std::string& column, const WindowData& window_data) const;
    double compute_avg(const std::string& column, const WindowData& window_data) const;
    double compute_min(const std::string& column, const WindowData& window_data) const;
    double compute_max(const std::string& column, const WindowData& window_data) const;
    double compute_count(const WindowData& window_data) const;
    double compute_stddev(const std::string& column, const WindowData& window_data) const;
};

} // namespace streaming_compute