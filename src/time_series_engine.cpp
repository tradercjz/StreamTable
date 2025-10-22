#include "streaming_compute/time_series_engine.h"
#include <arrow/compute/api.h>
#include <sstream>
#include <algorithm>
#include <numeric>
#include <cmath>

namespace streaming_compute {

TimeSeriesEngine::TimeSeriesEngine(const std::string& name,
                                  const std::string& metrics,
                                  std::shared_ptr<StreamTable> dummy_table,
                                  std::shared_ptr<StreamTable> output_table,
                                  const std::string& key_column,
                                  size_t window_size)
    : StreamEngine(name, dummy_table, output_table, key_column),
      metrics_(metrics), window_size_(window_size) {
    parse_metrics();
}

bool TimeSeriesEngine::process_message(const StreamMessage& message) {
    if (!active_ || !message.batch) {
        update_statistics(false);
        return false;
    }

    try {
        std::string key = extract_key(message.batch);
        
        {
            std::unique_lock<std::shared_mutex> lock(windows_mutex_);
            
            // Get or create window for this key
            WindowData& window = key_windows_[key];
            
            // Add new batch to window
            window.add_batch(message.batch);
            
            // Remove old batches if window is too large
            window.remove_old_batches(window_size_);
            
            // Compute aggregations
            window.compute_aggregations(parsed_metrics_);
            
            aggregations_computed_++;
        }
        
        // Create output batch
        RecordBatchPtr output_batch;
        {
            std::shared_lock<std::shared_mutex> lock(windows_mutex_);
            const WindowData& window = key_windows_[key];
            output_batch = create_output_batch(key, window);
        }
        
        // Send to output table
        if (output_batch && output_table_) {
            output_table_->append(output_batch);
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

std::unordered_map<std::string, int64_t> TimeSeriesEngine::get_stats() const {
    auto stats = StreamEngine::get_stats();
    
    stats["windows_created"] = windows_created_.load();
    stats["aggregations_computed"] = aggregations_computed_.load();
    stats["active_windows"] = key_windows_.size();
    stats["window_size"] = window_size_;
    
    return stats;
}

void TimeSeriesEngine::reset() {
    StreamEngine::reset();
    
    std::unique_lock<std::shared_mutex> lock(windows_mutex_);
    key_windows_.clear();
    windows_created_ = 0;
    aggregations_computed_ = 0;
}

void TimeSeriesEngine::set_window_size(size_t window_size) {
    window_size_ = window_size;
    
    // Update existing windows
    std::unique_lock<std::shared_mutex> lock(windows_mutex_);
    for (auto& pair : key_windows_) {
        pair.second.remove_old_batches(window_size_);
    }
}

bool TimeSeriesEngine::serialize_state(std::ostream& out) const {
    if (!StreamEngine::serialize_state(out)) {
        return false;
    }
    
    // Serialize window size
    out.write(reinterpret_cast<const char*>(&window_size_), sizeof(window_size_));
    
    // Serialize metrics
    size_t metrics_size = metrics_.size();
    out.write(reinterpret_cast<const char*>(&metrics_size), sizeof(metrics_size));
    out.write(metrics_.c_str(), metrics_size);
    
    // Serialize window data (simplified - in practice you'd serialize the actual data)
    size_t num_windows = key_windows_.size();
    out.write(reinterpret_cast<const char*>(&num_windows), sizeof(num_windows));
    
    for (const auto& pair : key_windows_) {
        const std::string& key = pair.first;
        const WindowData& window = pair.second;
        
        size_t key_size = key.size();
        out.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        out.write(key.c_str(), key_size);
        
        out.write(reinterpret_cast<const char*>(&window.total_rows), sizeof(window.total_rows));
        
        // Serialize aggregated values
        size_t num_values = window.aggregated_values.size();
        out.write(reinterpret_cast<const char*>(&num_values), sizeof(num_values));
        
        for (const auto& value_pair : window.aggregated_values) {
            size_t name_size = value_pair.first.size();
            out.write(reinterpret_cast<const char*>(&name_size), sizeof(name_size));
            out.write(value_pair.first.c_str(), name_size);
            out.write(reinterpret_cast<const char*>(&value_pair.second), sizeof(value_pair.second));
        }
    }
    
    return !out.fail();
}

bool TimeSeriesEngine::deserialize_state(std::istream& in) {
    if (!StreamEngine::deserialize_state(in)) {
        return false;
    }
    
    // Deserialize window size
    in.read(reinterpret_cast<char*>(&window_size_), sizeof(window_size_));
    
    // Deserialize metrics
    size_t metrics_size;
    in.read(reinterpret_cast<char*>(&metrics_size), sizeof(metrics_size));
    
    metrics_.resize(metrics_size);
    in.read(&metrics_[0], metrics_size);
    
    // Deserialize window data
    size_t num_windows;
    in.read(reinterpret_cast<char*>(&num_windows), sizeof(num_windows));
    
    key_windows_.clear();
    
    for (size_t i = 0; i < num_windows; ++i) {
        size_t key_size;
        in.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));
        
        std::string key(key_size, '\0');
        in.read(&key[0], key_size);
        
        WindowData& window = key_windows_[key];
        in.read(reinterpret_cast<char*>(&window.total_rows), sizeof(window.total_rows));
        
        // Deserialize aggregated values
        size_t num_values;
        in.read(reinterpret_cast<char*>(&num_values), sizeof(num_values));
        
        for (size_t j = 0; j < num_values; ++j) {
            size_t name_size;
            in.read(reinterpret_cast<char*>(&name_size), sizeof(name_size));
            
            std::string name(name_size, '\0');
            in.read(&name[0], name_size);
            
            double value;
            in.read(reinterpret_cast<char*>(&value), sizeof(value));
            
            window.aggregated_values[name] = value;
        }
    }
    
    parse_metrics();
    
    return !in.fail();
}

void TimeSeriesEngine::parse_metrics() {
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

std::string TimeSeriesEngine::extract_key(RecordBatchPtr batch) const {
    if (key_column_.empty()) {
        return "default";
    }
    
    auto column = batch->GetColumnByName(key_column_);
    if (!column || batch->num_rows() == 0) {
        return "default";
    }
    
    // Simplified key extraction - take first value
    // In practice, you'd handle different data types properly
    return column->ToString();
}

RecordBatchPtr TimeSeriesEngine::create_output_batch(const std::string& key, 
                                                    const WindowData& window_data) const {
    // Create output schema if not already defined
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
    
    if (!key_column_.empty()) {
        arrow::StringBuilder key_builder;
        key_builder.Append(key);
        ArrayPtr key_array;
        key_builder.Finish(&key_array);
        arrays.push_back(key_array);
    }
    
    for (const auto& metric : parsed_metrics_) {
        arrow::DoubleBuilder value_builder;
        double value = compute_metric(metric, window_data);
        value_builder.Append(value);
        ArrayPtr value_array;
        value_builder.Finish(&value_array);
        arrays.push_back(value_array);
    }
    
    return arrow::RecordBatch::Make(schema, 1, arrays);
}

double TimeSeriesEngine::compute_metric(const std::string& metric, const WindowData& window_data) const {
    // Check if already computed
    auto it = window_data.aggregated_values.find(metric);
    if (it != window_data.aggregated_values.end()) {
        return it->second;
    }
    
    // Parse metric function and column
    size_t paren_pos = metric.find('(');
    if (paren_pos == std::string::npos) {
        return 0.0;
    }
    
    std::string function = metric.substr(0, paren_pos);
    std::string column = metric.substr(paren_pos + 1);
    column = column.substr(0, column.find(')'));
    
    if (function == "sum") {
        return compute_sum(column, window_data);
    } else if (function == "avg" || function == "mean") {
        return compute_avg(column, window_data);
    } else if (function == "min") {
        return compute_min(column, window_data);
    } else if (function == "max") {
        return compute_max(column, window_data);
    } else if (function == "count") {
        return compute_count(window_data);
    } else if (function == "stddev" || function == "std") {
        return compute_stddev(column, window_data);
    }
    
    return 0.0;
}

double TimeSeriesEngine::compute_sum(const std::string& column, const WindowData& window_data) const {
    double sum = 0.0;
    
    for (const auto& batch : window_data.batches) {
        auto col = batch->GetColumnByName(column);
        if (col) {
            // Simplified - assumes numeric column
            // In practice, you'd use Arrow compute functions
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                // This is a placeholder - actual implementation would extract values properly
                sum += 1.0; // Placeholder value
            }
        }
    }
    
    return sum;
}

double TimeSeriesEngine::compute_avg(const std::string& column, const WindowData& window_data) const {
    if (window_data.total_rows == 0) {
        return 0.0;
    }
    
    return compute_sum(column, window_data) / window_data.total_rows;
}

double TimeSeriesEngine::compute_min(const std::string& column, const WindowData& window_data) const {
    double min_val = std::numeric_limits<double>::max();
    bool found = false;
    
    for (const auto& batch : window_data.batches) {
        auto col = batch->GetColumnByName(column);
        if (col) {
            // Simplified implementation
            min_val = std::min(min_val, 0.0); // Placeholder
            found = true;
        }
    }
    
    return found ? min_val : 0.0;
}

double TimeSeriesEngine::compute_max(const std::string& column, const WindowData& window_data) const {
    double max_val = std::numeric_limits<double>::lowest();
    bool found = false;
    
    for (const auto& batch : window_data.batches) {
        auto col = batch->GetColumnByName(column);
        if (col) {
            // Simplified implementation
            max_val = std::max(max_val, 1.0); // Placeholder
            found = true;
        }
    }
    
    return found ? max_val : 0.0;
}

double TimeSeriesEngine::compute_count(const WindowData& window_data) const {
    return static_cast<double>(window_data.total_rows);
}

double TimeSeriesEngine::compute_stddev(const std::string& column, const WindowData& window_data) const {
    if (window_data.total_rows <= 1) {
        return 0.0;
    }
    
    double mean = compute_avg(column, window_data);
    double sum_sq_diff = 0.0;
    
    for (const auto& batch : window_data.batches) {
        auto col = batch->GetColumnByName(column);
        if (col) {
            // Simplified implementation
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                double value = 1.0; // Placeholder
                sum_sq_diff += (value - mean) * (value - mean);
            }
        }
    }
    
    return std::sqrt(sum_sq_diff / (window_data.total_rows - 1));
}

// WindowData methods
void TimeSeriesEngine::WindowData::add_batch(RecordBatchPtr batch) {
    batches.push_back(batch);
    total_rows += batch->num_rows();
}

void TimeSeriesEngine::WindowData::remove_old_batches(size_t max_size) {
    while (total_rows > static_cast<int64_t>(max_size) && !batches.empty()) {
        total_rows -= batches.front()->num_rows();
        batches.pop_front();
    }
}

void TimeSeriesEngine::WindowData::compute_aggregations(const std::vector<std::string>& metrics) {
    // This would compute and cache aggregation results
    // For now, we'll leave it empty as the actual computation happens in compute_metric
}

} // namespace streaming_compute