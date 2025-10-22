#include "streaming_compute/cross_sectional_engine.h"
#include <sstream>
#include <algorithm>
#include <numeric>
#include <cmath>

namespace streaming_compute {

CrossSectionalEngine::CrossSectionalEngine(const std::string& name,
                                          const std::string& metrics,
                                          std::shared_ptr<StreamTable> dummy_table,
                                          std::shared_ptr<StreamTable> output_table,
                                          const std::string& key_column,
                                          const std::string& triggering_pattern,
                                          size_t triggering_interval,
                                          const std::string& time_column)
    : StreamEngine(name, dummy_table, output_table, key_column),
      metrics_(metrics), triggering_pattern_(triggering_pattern),
      triggering_interval_(triggering_interval), time_column_(time_column),
      last_trigger_time_(std::chrono::system_clock::now()) {
    parse_metrics();
}

bool CrossSectionalEngine::process_message(const StreamMessage& message) {
    if (!active_ || !message.batch) {
        update_statistics(false);
        return false;
    }

    try {
        std::string key = extract_key(message.batch);
        
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            
            // Add batch to cross-sectional data
            cross_sectional_data_.add_batch(message.batch, key);
            
            messages_since_trigger_++;
        }
        
        // Check if we should trigger cross-sectional computation
        if (should_trigger()) {
            execute_cross_sectional_computation();
        }
        
        last_processed_msg_id_ = message.offset;
        update_statistics(true);
        create_snapshot_if_needed();
        
        return true;
        
    } catch (const std::exception& e) {
        update_statistics(false);
        return false;
    }
}

std::unordered_map<std::string, int64_t> CrossSectionalEngine::get_stats() const {
    auto stats = StreamEngine::get_stats();
    
    stats["triggers_executed"] = triggers_executed_.load();
    stats["cross_sectional_computations"] = cross_sectional_computations_.load();
    stats["messages_since_trigger"] = messages_since_trigger_.load();
    stats["triggering_interval"] = triggering_interval_;
    stats["accumulated_rows"] = cross_sectional_data_.total_rows;
    
    return stats;
}

void CrossSectionalEngine::reset() {
    StreamEngine::reset();
    
    std::lock_guard<std::mutex> lock(data_mutex_);
    cross_sectional_data_.clear();
    messages_since_trigger_ = 0;
    triggers_executed_ = 0;
    cross_sectional_computations_ = 0;
    last_trigger_time_ = std::chrono::system_clock::now();
}

bool CrossSectionalEngine::force_trigger() {
    execute_cross_sectional_computation();
    return true;
}

void CrossSectionalEngine::set_triggering_interval(size_t interval) {
    triggering_interval_ = interval;
}

bool CrossSectionalEngine::serialize_state(std::ostream& out) const {
    if (!StreamEngine::serialize_state(out)) {
        return false;
    }
    
    // Serialize configuration
    size_t metrics_size = metrics_.size();
    out.write(reinterpret_cast<const char*>(&metrics_size), sizeof(metrics_size));
    out.write(metrics_.c_str(), metrics_size);
    
    size_t pattern_size = triggering_pattern_.size();
    out.write(reinterpret_cast<const char*>(&pattern_size), sizeof(pattern_size));
    out.write(triggering_pattern_.c_str(), pattern_size);
    
    out.write(reinterpret_cast<const char*>(&triggering_interval_), sizeof(triggering_interval_));
    
    size_t time_col_size = time_column_.size();
    out.write(reinterpret_cast<const char*>(&time_col_size), sizeof(time_col_size));
    out.write(time_column_.c_str(), time_col_size);
    
    // Serialize state
    size_t messages_since = messages_since_trigger_.load();
    out.write(reinterpret_cast<const char*>(&messages_since), sizeof(messages_since));
    
    out.write(reinterpret_cast<const char*>(&cross_sectional_data_.total_rows), 
              sizeof(cross_sectional_data_.total_rows));
    
    return !out.fail();
}

bool CrossSectionalEngine::deserialize_state(std::istream& in) {
    if (!StreamEngine::deserialize_state(in)) {
        return false;
    }
    
    // Deserialize configuration
    size_t metrics_size;
    in.read(reinterpret_cast<char*>(&metrics_size), sizeof(metrics_size));
    
    metrics_.resize(metrics_size);
    in.read(&metrics_[0], metrics_size);
    
    size_t pattern_size;
    in.read(reinterpret_cast<char*>(&pattern_size), sizeof(pattern_size));
    
    triggering_pattern_.resize(pattern_size);
    in.read(&triggering_pattern_[0], pattern_size);
    
    in.read(reinterpret_cast<char*>(&triggering_interval_), sizeof(triggering_interval_));
    
    size_t time_col_size;
    in.read(reinterpret_cast<char*>(&time_col_size), sizeof(time_col_size));
    
    time_column_.resize(time_col_size);
    in.read(&time_column_[0], time_col_size);
    
    // Deserialize state
    size_t messages_since;
    in.read(reinterpret_cast<char*>(&messages_since), sizeof(messages_since));
    messages_since_trigger_ = messages_since;
    
    in.read(reinterpret_cast<char*>(&cross_sectional_data_.total_rows), 
            sizeof(cross_sectional_data_.total_rows));
    
    parse_metrics();
    last_trigger_time_ = std::chrono::system_clock::now();
    
    return !in.fail();
}

void CrossSectionalEngine::parse_metrics() {
    parsed_metrics_.clear();
    
    std::istringstream iss(metrics_);
    std::string metric;
    
    while (std::getline(iss, metric, ',')) {
        // Trim whitespace
        metric.erase(0, metric.find_first_not_of(" \t"));
        metric.erase(metric.find_last_not_of(" \t") + 1);
        
        if (!metric.empty()) {
            parsed_metrics_.push_back(metric);
        }
    }
}

std::string CrossSectionalEngine::extract_key(RecordBatchPtr batch) const {
    if (key_column_.empty()) {
        return "default";
    }
    
    auto column = batch->GetColumnByName(key_column_);
    if (!column || batch->num_rows() == 0) {
        return "default";
    }
    
    // Simplified key extraction - take first value
    return column->ToString();
}

bool CrossSectionalEngine::should_trigger() const {
    if (triggering_pattern_ == "keyCount") {
        return messages_since_trigger_.load() >= triggering_interval_;
    } else if (triggering_pattern_ == "time") {
        auto now = std::chrono::system_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - last_trigger_time_).count();
        return elapsed >= static_cast<int64_t>(triggering_interval_);
    } else if (triggering_pattern_ == "perBatch") {
        return true; // Trigger on every batch
    }
    
    return false;
}

void CrossSectionalEngine::execute_cross_sectional_computation() {
    std::lock_guard<std::mutex> lock(data_mutex_);
    
    if (cross_sectional_data_.batches.empty()) {
        return;
    }
    
    // Extract column values for cross-sectional analysis
    cross_sectional_data_.extract_column_values();
    
    // Create output batch
    RecordBatchPtr output_batch = create_output_batch(cross_sectional_data_);
    
    // Send to output table
    if (output_batch && output_table_) {
        output_table_->append(output_batch);
    }
    
    // Clear accumulated data
    cross_sectional_data_.clear();
    
    // Reset trigger state
    messages_since_trigger_ = 0;
    last_trigger_time_ = std::chrono::system_clock::now();
    
    triggers_executed_++;
    cross_sectional_computations_++;
}

RecordBatchPtr CrossSectionalEngine::create_output_batch(const CrossSectionalData& data) const {
    if (data.keys.empty()) {
        return nullptr;
    }
    
    // Create output schema
    std::vector<std::shared_ptr<arrow::Field>> fields;
    
    if (!key_column_.empty()) {
        fields.push_back(arrow::field(key_column_, arrow::utf8()));
    }
    
    for (const auto& metric : parsed_metrics_) {
        fields.push_back(arrow::field(metric, arrow::float64()));
    }
    
    auto schema = arrow::schema(fields);
    
    // Create arrays
    std::vector<ArrayPtr> arrays;
    
    // Key column
    if (!key_column_.empty()) {
        arrow::StringBuilder key_builder;
        for (const auto& key : data.keys) {
            key_builder.Append(key);
        }
        ArrayPtr key_array;
        key_builder.Finish(&key_array);
        arrays.push_back(key_array);
    }
    
    // Metric columns
    for (const auto& metric : parsed_metrics_) {
        arrow::DoubleBuilder value_builder;
        
        std::vector<double> metric_values = compute_metric(metric, data);
        
        for (double value : metric_values) {
            value_builder.Append(value);
        }
        
        ArrayPtr value_array;
        value_builder.Finish(&value_array);
        arrays.push_back(value_array);
    }
    
    return arrow::RecordBatch::Make(schema, data.keys.size(), arrays);
}

std::vector<double> CrossSectionalEngine::compute_metric(const std::string& metric,
                                                        const CrossSectionalData& data) const {
    // Parse metric function and column
    size_t paren_pos = metric.find('(');
    if (paren_pos == std::string::npos) {
        return std::vector<double>(data.keys.size(), 0.0);
    }
    
    std::string function = metric.substr(0, paren_pos);
    std::string params = metric.substr(paren_pos + 1);
    params = params.substr(0, params.find(')'));
    
    // Extract column name and optional parameters
    std::string column = params;
    bool percent = false;
    double percentile = 0.5;
    
    // Check for additional parameters
    size_t comma_pos = params.find(',');
    if (comma_pos != std::string::npos) {
        column = params.substr(0, comma_pos);
        std::string param = params.substr(comma_pos + 1);
        param.erase(0, param.find_first_not_of(" \t"));
        param.erase(param.find_last_not_of(" \t") + 1);
        
        if (param == "percent=true" || param == "true") {
            percent = true;
        } else {
            percentile = std::stod(param);
        }
    }
    
    // Get column values
    std::vector<double> values = extract_column_values(column, data);
    
    if (function == "rank") {
        return compute_rank(values, percent);
    } else if (function == "percentile_rank") {
        return compute_percentile_rank(values);
    } else if (function == "zscore") {
        return compute_zscore(values);
    } else if (function == "quantile_normalize") {
        return compute_quantile_normalize(values);
    } else if (function == "percentile") {
        double p = compute_percentile(values, percentile);
        return std::vector<double>(values.size(), p);
    }
    
    return std::vector<double>(data.keys.size(), 0.0);
}

std::vector<double> CrossSectionalEngine::extract_column_values(const std::string& column,
                                                               const CrossSectionalData& data) const {
    auto it = data.column_values.find(column);
    if (it != data.column_values.end()) {
        return it->second;
    }
    
    // If not pre-extracted, extract now (simplified implementation)
    std::vector<double> values;
    for (const auto& batch : data.batches) {
        auto col = batch->GetColumnByName(column);
        if (col) {
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                values.push_back(1.0); // Placeholder value
            }
        }
    }
    
    return values;
}

std::vector<double> CrossSectionalEngine::compute_rank(const std::vector<double>& values, bool percent) const {
    std::vector<std::pair<double, size_t>> indexed_values;
    for (size_t i = 0; i < values.size(); ++i) {
        indexed_values.emplace_back(values[i], i);
    }
    
    // Sort by value
    std::sort(indexed_values.begin(), indexed_values.end());
    
    std::vector<double> ranks(values.size());
    
    for (size_t i = 0; i < indexed_values.size(); ++i) {
        size_t original_index = indexed_values[i].second;
        double rank = static_cast<double>(i + 1);
        
        if (percent) {
            rank = rank / values.size();
        }
        
        ranks[original_index] = rank;
    }
    
    return ranks;
}

std::vector<double> CrossSectionalEngine::compute_percentile_rank(const std::vector<double>& values) const {
    return compute_rank(values, true);
}

std::vector<double> CrossSectionalEngine::compute_zscore(const std::vector<double>& values) const {
    if (values.empty()) {
        return {};
    }
    
    double mean = compute_cross_sectional_mean(values);
    double std = compute_cross_sectional_std(values);
    
    std::vector<double> zscores;
    zscores.reserve(values.size());
    
    for (double value : values) {
        if (std > 0) {
            zscores.push_back((value - mean) / std);
        } else {
            zscores.push_back(0.0);
        }
    }
    
    return zscores;
}

std::vector<double> CrossSectionalEngine::compute_quantile_normalize(const std::vector<double>& values) const {
    if (values.empty()) {
        return {};
    }
    
    // Create sorted indices
    std::vector<size_t> indices(values.size());
    std::iota(indices.begin(), indices.end(), 0);
    
    std::sort(indices.begin(), indices.end(),
              [&values](size_t a, size_t b) { return values[a] < values[b]; });
    
    std::vector<double> normalized(values.size());
    
    for (size_t i = 0; i < indices.size(); ++i) {
        double quantile = static_cast<double>(i) / (values.size() - 1);
        normalized[indices[i]] = quantile;
    }
    
    return normalized;
}

double CrossSectionalEngine::compute_percentile(const std::vector<double>& values, double percentile) const {
    if (values.empty()) {
        return 0.0;
    }
    
    std::vector<double> sorted_values = values;
    std::sort(sorted_values.begin(), sorted_values.end());
    
    double index = percentile * (sorted_values.size() - 1);
    size_t lower_index = static_cast<size_t>(std::floor(index));
    size_t upper_index = static_cast<size_t>(std::ceil(index));
    
    if (lower_index == upper_index) {
        return sorted_values[lower_index];
    }
    
    double weight = index - lower_index;
    return sorted_values[lower_index] * (1.0 - weight) + sorted_values[upper_index] * weight;
}

double CrossSectionalEngine::compute_cross_sectional_mean(const std::vector<double>& values) const {
    if (values.empty()) {
        return 0.0;
    }
    
    return std::accumulate(values.begin(), values.end(), 0.0) / values.size();
}

double CrossSectionalEngine::compute_cross_sectional_std(const std::vector<double>& values) const {
    if (values.size() <= 1) {
        return 0.0;
    }
    
    double mean = compute_cross_sectional_mean(values);
    double sum_sq_diff = 0.0;
    
    for (double value : values) {
        sum_sq_diff += (value - mean) * (value - mean);
    }
    
    return std::sqrt(sum_sq_diff / (values.size() - 1));
}

// CrossSectionalData methods
void CrossSectionalEngine::CrossSectionalData::add_batch(RecordBatchPtr batch, const std::string& key) {
    batches.push_back(batch);
    keys.push_back(key);
    total_rows += batch->num_rows();
    last_update = std::chrono::system_clock::now();
}

void CrossSectionalEngine::CrossSectionalData::clear() {
    batches.clear();
    column_values.clear();
    keys.clear();
    total_rows = 0;
    last_update = std::chrono::system_clock::now();
}

void CrossSectionalEngine::CrossSectionalData::extract_column_values() {
    column_values.clear();
    
    // Extract values for each column across all batches
    for (size_t batch_idx = 0; batch_idx < batches.size(); ++batch_idx) {
        const auto& batch = batches[batch_idx];
        
        for (int i = 0; i < batch->schema()->num_fields(); ++i) {
            const std::string& column_name = batch->schema()->field(i)->name();
            auto column = batch->column(i);
            
            // Simplified extraction - assumes numeric columns
            for (int64_t row = 0; row < batch->num_rows(); ++row) {
                column_values[column_name].push_back(1.0); // Placeholder value
            }
        }
    }
}

} // namespace streaming_compute