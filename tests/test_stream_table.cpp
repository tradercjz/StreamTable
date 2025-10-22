#include <gtest/gtest.h>
#include "streaming_compute/streaming_compute.h"

using namespace streaming_compute;

class StreamTableTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test schema
        std::vector<std::shared_ptr<arrow::Field>> fields = {
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("value", arrow::float64())
        };
        schema_ = arrow::schema(fields);
        
        config_.max_memory_size = 1000;
        config_.enable_persistence = false;
    }
    
    RecordBatchPtr create_test_batch(int64_t id, const std::string& name, double value) {
        arrow::Int64Builder id_builder;
        id_builder.Append(id);
        ArrayPtr id_array;
        id_builder.Finish(&id_array);
        
        arrow::StringBuilder name_builder;
        name_builder.Append(name);
        ArrayPtr name_array;
        name_builder.Finish(&name_array);
        
        arrow::DoubleBuilder value_builder;
        value_builder.Append(value);
        ArrayPtr value_array;
        value_builder.Finish(&value_array);
        
        std::vector<ArrayPtr> arrays = {id_array, name_array, value_array};
        return arrow::RecordBatch::Make(schema_, 1, arrays);
    }
    
    SchemaPtr schema_;
    StreamConfig config_;
};

TEST_F(StreamTableTest, BasicCreation) {
    StreamTable table("test_table", schema_, config_);
    
    EXPECT_EQ(table.name(), "test_table");
    EXPECT_EQ(table.size(), 0);
    EXPECT_EQ(table.current_offset(), 0);
    EXPECT_FALSE(table.is_shared());
}

TEST_F(StreamTableTest, AppendAndSelect) {
    StreamTable table("test_table", schema_, config_);
    
    // Append some data
    auto batch1 = create_test_batch(1, "test1", 10.5);
    auto batch2 = create_test_batch(2, "test2", 20.5);
    
    EXPECT_TRUE(table.append(batch1));
    EXPECT_TRUE(table.append(batch2));
    
    EXPECT_EQ(table.size(), 2);
    EXPECT_EQ(table.current_offset(), 2);
    
    // Select data
    auto batches = table.select(0, -1);
    EXPECT_EQ(batches.size(), 2);
    
    // Select with limit
    auto limited_batches = table.select(0, 1);
    EXPECT_EQ(limited_batches.size(), 1);
}

TEST_F(StreamTableTest, ShareAndUnshare) {
    StreamTable table("test_table", schema_, config_);
    
    EXPECT_FALSE(table.is_shared());
    
    table.share("shared_table");
    EXPECT_TRUE(table.is_shared());
    
    table.unshare();
    EXPECT_FALSE(table.is_shared());
}

TEST_F(StreamTableTest, KeyedStreamTable) {
    std::vector<std::string> key_columns = {"id"};
    KeyedStreamTable keyed_table("keyed_table", key_columns, schema_, config_);
    
    auto batch1 = create_test_batch(1, "test1", 10.5);
    auto batch2 = create_test_batch(1, "test1_duplicate", 15.5); // Same key
    
    EXPECT_TRUE(keyed_table.append(batch1));
    EXPECT_FALSE(keyed_table.append(batch2)); // Should reject duplicate key
    
    EXPECT_EQ(keyed_table.size(), 1);
}

TEST_F(StreamTableTest, Statistics) {
    StreamTable table("test_table", schema_, config_);
    
    auto batch = create_test_batch(1, "test", 10.5);
    table.append(batch);
    
    auto stats = table.get_stats();
    EXPECT_GT(stats["total_appends"], 0);
    EXPECT_EQ(stats["total_rows"], 1);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}