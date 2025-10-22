#include "streaming_compute/reactive_state_engine.h"
#include <sstream>
#include <algorithm>

namespace streaming_compute {

ReactiveStateEngine::ReactiveStateEngine(const std::string& name,
                                        const std::string& metrics,
                                        std::shared_ptr<StreamTable> dummy_table,
                                        std::shared_ptr<StreamTable> output_table,
                                        const std::string& key_column)
    : StreamEngine(name, dummy_table, output_table, key_column),
      metrics_(metrics) {
    parse_metrics();
}

bool ReactiveStateEngine::process_message(const StreamMessage& message) {
    if (!active_ || !message.batch) {
        update_statistics(false);
        return false;
    }

    try {
        std::string key = extract_key(message.batch);
        
        // Update state for this key
        update_state(key, message.batch);
        
        // Create output batch
        RecordBatchPtr output_batch;
        {
            std::shared_lock<std::shared_mutex> lock(states_mutex_);
            const KeyState& state = key_states_[key];
            output_batch = create_output_batch(key, state);
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

std::unordered_map<std::string, int64_t> ReactiveStateEngine::get_stats() const {
    auto stats = StreamEngine::get_stats();
    
    stats["states_created"] = states_created_.load();
    stats["state_updates"] = state_updates_.load();
    stats["active_states"] = key_states_.size();
    
    return stats;
}

void ReactiveStateEngine::reset() {
    StreamEngine::reset();
    
    std::unique_lock<std::shared_mutex> lock(states_mutex_);
    key_states_.clear();
    states_created_ = 0;
    state_updates_ = 0;
}

std::unordered_map<std::string, double> ReactiveStateEngine::get_key_state(const std::string& key) const {
    std::shared_lock<std::shared_mutex> lock(states_mutex_);
    
    auto it = key_states_.find(key);
    if (it != key_states_.end()) {
        return it->second.values;
    }
    
    return {};
}

void ReactiveStateEngine::set_key_state(const std::string& key, 
                                       const std::unordered_map<std::string, double>& state) {
    std::unique_lock<std::shared_mutex> lock(states_mutex_);
    
    KeyState& key_state = key_states_[key];
    key_state.values = state;
    key_state.last_update = std::chrono::system_clock::now();
}

bool ReactiveStateEngine::serialize_state(std::ostream& out) const {
    if (!StreamEngine::serialize_state(out)) {
        return false;
    }
    
    // Serialize metrics
    size_t metrics_size = metrics_.size();
    out.write(reinterpret_cast<const char*>(&metrics_size), sizeof(metrics_size));
    out.write(metrics_.c_str(), metrics_size);
    
    // Serialize key states
    size_t num_states = key_states_.size();
    out.write(reinterpret_cast<const char*>(&num_states), sizeof(num_states));
    
    for (const auto& pair : key_states_) {
        const std::string& key = pair.first;
        const KeyState& state = pair.second;
        
        // Serialize key
        size_t key_size = key.size();
        out.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        out.write(key.c_str(), key_size);
        
        // Serialize state values
        size_t num_values = state.values.size();
        out.write(reinterpret_cast<const char*>(&num_values), sizeof(num_values));
        
        for (const auto& value_pair : state.values) {
            size_t name_size = value_pair.first.size();
            out.write(reinterpret_cast<const char*>(&name_size), sizeof(name_size));
            out.write(value_pair.first.c_str(), name_size);
            out.write(reinterpret_cast<const char*>(&value_pair.second), sizeof(value_pair.second));
        }
        
        // Serialize counts
        size_t num_counts = state.counts.size();
        out.write(reinterpret_cast<const char*>(&num_counts), sizeof(num_counts));
        
        for (const auto& count_pair : state.counts) {
            size_t name_size = count_pair.first.size();
            out.write(reinterpret_cast<const char*>(&name_size), sizeof(name_size));
            out.write(count_pair.first.c_str(), name_size);
            out.write(reinterpret_cast<const char*>(&count_pair.second), sizeof(count_pair.second));
        }
        
        // Serialize total messages
        out.write(reinterpret_cast<const char*>(&state.total_messages), sizeof(state.total_messages));
    }
    
    return !out.fail();
}

bool ReactiveStateEngine::deserialize_state(std::istream& in) {
    if (!StreamEngine::deserialize_state(in)) {
        return false;
    }
    
    // Deserialize metrics
    size_t metrics_size;
    in.read(reinterpret_cast<char*>(&metrics_size), sizeof(metrics_size));
    
    metrics_.resize(metrics_size);
    in.read(&metrics_[0], metrics_size);
    
    // Deserialize key states
    size_t num_states;
    in.read(reinterpret_cast<char*>(&num_states), sizeof(num_states));
    
    key_states_.clear();
    
    for (size_t i = 0; i < num_states; ++i) {
        // Deserialize key
        size_t key_size;
        in.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));
        
        std::string key(key_size, '\0');
        in.read(&key[0], key_size);
        
        KeyState& state = key_states_[key];
        
        // Deserialize state values
        size_t num_values;
        in.read(reinterpret_cast<char*>(&num_values), sizeof(num_values));
        
        for (size_t j = 0; j < num_values; ++j) {
            size_t name_size;
            in.read(reinterpret_cast<char*>(&name_size), sizeof(name_size));
            
            std::string name(name_size, '\0');
            in.read(&name[0], name_size);
            
            double value;
            in.read(reinterpret_cast<char*>(&value), sizeof(value));
            
            state.values[name] = value;
        }
        
        // Deserialize counts
        size_t num_counts;
        in.read(reinterpret_cast<char*>(&num_counts), sizeof(num_counts));
        
        for (size_t j = 0; j < num_counts; ++j) {
            size_t name_size;
            in.read(reinterpret_cast<char*>(&name_size), sizeof(name_size));
            
            std::string name(name_size, '\0');
            in.read(&name[0], name_size);
            
            int64_t count;
            in.read(reinterpret_cast<char*>(&count), sizeof(count));
            
            state.counts[name] = count;
        }
        
        // Deserialize total messages
        in.read(reinterpret_cast<char*>(&state.total_messages), sizeof(state.total_messages));
        
        state.last_update = std::chrono::system_clock::now();
    }
    
    parse_metrics();
    
    return !in.fail();
}

void ReactiveStateEngine::parse_metrics() {
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

std::string ReactiveStateEngine::extract_key(RecordBatchPtr batch) const {
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

RecordBatchPtr ReactiveStateEngine::create_output_batch(const std::string& key, 
                                                       const KeyState& state) const {
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
    
    if (!key_column_.empty()) {
        arrow::StringBuilder key_builder;
        key_builder.Append(key);
        ArrayPtr key_array;
        key_builder.Finish(&key_array);
        arrays.push_back(key_array);
    }
    
    for (const auto& metric : parsed_metrics_) {
        arrow::DoubleBuilder value_builder;
        double value = state.get_metric_value(metric);
        value_builder.Append(value);
        ArrayPtr value_array;
        value_builder.Finish(&value_array);
        arrays.push_back(value_array);
    }
    
    return arrow::RecordBatch::Make(schema, 1, arrays);
}

void ReactiveStateEngine::update_state(const std::string& key, RecordBatchPtr batch) {
    std::unique_lock<std::shared_mutex> lock(states_mutex_);
    
    KeyState& state = key_states_[key];
    
    if (state.total_messages == 0) {
        states_created_++;
    }
    
    state.total_messages++;
    state.last_update = std::chrono::system_clock::now();
    
    // Update metrics
    for (const auto& metric : parsed_metrics_) {
        // Parse metric function and column
        size_t paren_pos = metric.find('(');
        if (paren_pos == std::string::npos) {
            continue;
        }
        
        std::string function = metric.substr(0, paren_pos);
        std::string column = metric.substr(paren_pos + 1);
        column = column.substr(0, column.find(')'));
        
        double value = 0.0;
        
        if (function == "cumsum") {
            value = compute_cumsum(column, batch, state);
        } else if (function == "cumavg") {
            value = compute_cumavg(column, batch, state);
        } else if (function == "cummax") {
            value = compute_cummax(column, batch, state);
        } else if (function == "cummin") {
            value = compute_cummin(column, batch, state);
        } else if (function == "cumcount") {
            value = compute_cumcount(batch, state);
        } else if (function == "cumprod") {
            value = compute_cumprod(column, batch, state);
        } else if (function == "ewma") {
            value = compute_ewma(column, batch, state);
        } else {
            value = compute_custom_function(function, column, batch, state);
        }
        
        state.update_metric(metric, value);
    }
    
    state_updates_++;
}

double ReactiveStateEngine::compute_cumsum(const std::string& column, RecordBatchPtr batch, KeyState& state) {
    double current_sum = state.get_metric_value("cumsum(" + column + ")");
    
    // Extract values from batch and add to cumulative sum
    auto col = batch->GetColumnByName(column);
    if (col) {
        // Simplified - assumes numeric column
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            current_sum += 1.0; // Placeholder value
        }
    }
    
    return current_sum;
}

double ReactiveStateEngine::compute_cumavg(const std::string& column, RecordBatchPtr batch, KeyState& state) {
    double current_sum = state.get_metric_value("cumsum(" + column + ")");
    int64_t current_count = state.counts["cumavg(" + column + ")"];
    
    // Add new values
    auto col = batch->GetColumnByName(column);
    if (col) {
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            current_sum += 1.0; // Placeholder value
            current_count++;
        }
    }
    
    state.values["cumsum(" + column + ")"] = current_sum;
    state.counts["cumavg(" + column + ")"] = current_count;
    
    return current_count > 0 ? current_sum / current_count : 0.0;
}

double ReactiveStateEngine::compute_cummax(const std::string& column, RecordBatchPtr batch, KeyState& state) {
    double current_max = state.get_metric_value("cummax(" + column + ")");
    
    auto col = batch->GetColumnByName(column);
    if (col) {
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            double value = 1.0; // Placeholder value
            current_max = std::max(current_max, value);
        }
    }
    
    return current_max;
}

double ReactiveStateEngine::compute_cummin(const std::string& column, RecordBatchPtr batch, KeyState& state) {
    double current_min = state.get_metric_value("cummin(" + column + ")");
    
    // Initialize with first value if this is the first update
    if (state.counts["cummin(" + column + ")"] == 0) {
        current_min = std::numeric_limits<double>::max();
    }
    
    auto col = batch->GetColumnByName(column);
    if (col) {
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            double value = 1.0; // Placeholder value
            current_min = std::min(current_min, value);
        }
    }
    
    state.counts["cummin(" + column + ")"]++;
    
    return current_min;
}

double ReactiveStateEngine::compute_cumcount(RecordBatchPtr batch, KeyState& state) {
    int64_t current_count = state.counts["cumcount"];
    current_count += batch->num_rows();
    state.counts["cumcount"] = current_count;
    
    return static_cast<double>(current_count);
}

double ReactiveStateEngine::compute_cumprod(const std::string& column, RecordBatchPtr batch, KeyState& state) {
    double current_prod = state.get_metric_value("cumprod(" + column + ")");
    
    // Initialize with 1 if this is the first update
    if (state.counts["cumprod(" + column + ")"] == 0) {
        current_prod = 1.0;
    }
    
    auto col = batch->GetColumnByName(column);
    if (col) {
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            double value = 1.0; // Placeholder value
            current_prod *= value;
        }
    }
    
    state.counts["cumprod(" + column + ")"]++;
    
    return current_prod;
}

double ReactiveStateEngine::compute_ewma(const std::string& column, RecordBatchPtr batch, KeyState& state, double alpha) {
    double current_ewma = state.get_metric_value("ewma(" + column + ")");
    
    auto col = batch->GetColumnByName(column);
    if (col) {
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            double value = 1.0; // Placeholder value
            
            if (state.counts["ewma(" + column + ")"] == 0) {
                current_ewma = value;
            } else {
                current_ewma = alpha * value + (1.0 - alpha) * current_ewma;
            }
            
            state.counts["ewma(" + column + ")"]++;
        }
    }
    
    return current_ewma;
}

double ReactiveStateEngine::compute_custom_function(const std::string& function_name,
                                                   const std::string& column,
                                                   RecordBatchPtr batch,
                                                   KeyState& state) {
    // Placeholder for custom functions
    // Users could extend this to add their own stateful functions
    return 0.0;
}

// KeyState methods
void ReactiveStateEngine::KeyState::update_metric(const std::string& metric, double value) {
    values[metric] = value;
    last_update = std::chrono::system_clock::now();
}

double ReactiveStateEngine::KeyState::get_metric_value(const std::string& metric) const {
    auto it = values.find(metric);
    return it != values.end() ? it->second : 0.0;
}

} // namespace streaming_compute