#pragma once

#include "stream_engine.h"

namespace streaming_compute {

/**
 * CrossSectionalEngine - Performs cross-sectional analysis across multiple streams
 * 
 * Features:
 * - Cross-sectional aggregations (rank, percentile, etc.)
 * - Triggering patterns (keyCount, time-based)
 * - Batch processing of cross-sectional data
 * - Configurable triggering intervals
 */
class CrossSectionalEngine : public StreamEngine {
public:
    /**
     * Constructor
     * @param name Engine name
     * @param metrics Computation metrics (e.g., "rank(price), percentile(volume, 0.5)")
     * @param dummy_table Input table schema reference
     * @param output_table Output table for results
     * @param key_column Column name for partitioning
     * @param triggering_pattern Triggering pattern ("keyCount", "time", "perBatch")
     * @param triggering_interval Interval for triggering (count or milliseconds)
     * @param time_column Time column name for time-based triggering
     */
    CrossSectionalEngine(const std::string& name,
                        const std::string& metrics,
                        std::shared_ptr<StreamTable> dummy_table,
                        std::shared_ptr<StreamTable> output_table,
                        const std::string& key_column = "",
                        const std::string& triggering_pattern = "keyCount",
                        size_t triggering_interval = 1000,
                        const std::string& time_column = "");

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
     * Force trigger cross-sectional computation
     * @return true if successful, false otherwise
     */
    bool force_trigger();

    /**
     * Set triggering interval
     * @param interval New triggering interval
     */
    void set_triggering_interval(size_t interval);

    /**
     * Get current triggering interval
     * @return Triggering interval
     */
    size_t get_triggering_interval() const { return triggering_interval_; }

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
    struct CrossSectionalData {
        std::vector<RecordBatchPtr> batches;
        std::unordered_map<std::string, std::vector<double>> column_values;
        std::vector<std::string> keys;
        int64_t total_rows = 0;
        std::chrono::system_clock::time_point last_update;
        
        CrossSectionalData() : last_update(std::chrono::system_clock::now()) {}
        
        void add_batch(RecordBatchPtr batch, const std::string& key);
        void clear();
        void extract_column_values();
    };

    std::string metrics_;
    std::vector<std::string> parsed_metrics_;
    std::string triggering_pattern_;
    size_t triggering_interval_;
    std::string time_column_;
    
    // Cross-sectional data accumulation
    CrossSectionalData cross_sectional_data_;
    mutable std::mutex data_mutex_;
    
    // Triggering state
    std::atomic<size_t> messages_since_trigger_{0};
    std::chrono::system_clock::time_point last_trigger_time_;
    std::atomic<bool> trigger_pending_{false};
    
    // Statistics
    std::atomic<int64_t> triggers_executed_{0};
    std::atomic<int64_t> cross_sectional_computations_{0};
    
    // Helper methods
    void parse_metrics();
    std::string extract_key(RecordBatchPtr batch) const;
    bool should_trigger() const;
    void execute_cross_sectional_computation();
    RecordBatchPtr create_output_batch(const CrossSectionalData& data) const;
    
    // Cross-sectional computation methods
    std::vector<double> compute_rank(const std::vector<double>& values, bool percent = false) const;
    std::vector<double> compute_percentile_rank(const std::vector<double>& values) const;
    std::vector<double> compute_zscore(const std::vector<double>& values) const;
    std::vector<double> compute_quantile_normalize(const std::vector<double>& values) const;
    double compute_percentile(const std::vector<double>& values, double percentile) const;
    double compute_cross_sectional_mean(const std::vector<double>& values) const;
    double compute_cross_sectional_std(const std::vector<double>& values) const;
    
    // Metric computation
    std::vector<double> compute_metric(const std::string& metric, 
                                      const CrossSectionalData& data) const;
    std::vector<double> extract_column_values(const std::string& column, 
                                             const CrossSectionalData& data) const;
};

} // namespace streaming_compute