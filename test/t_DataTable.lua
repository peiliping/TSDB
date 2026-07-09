local DataFile = require("db.DataFile")
local DataTable = require("db.DataTable")
local TestTools = require("test.TestTools")

-- Real dependencies
local TimeCol = require("record.col.TimeCol")
local NumberCol = require("record.col.NumberCol")
local Columns = require("record.col.Columns")
local Record = require("record.Record")
local Batch = require("record.Batch")
local GroupBatch = require("record.GroupBatch")
local RingBuffer = require("aggregate.RingBuffer")
local BinaryTools = require("tools.BinaryTools") -- For unpacking header in DataFile tests
local Headers = require("db.Headers") -- For header length in DataFile tests

local t_DataTable = {}

local TEST_DIR = "./test_data/"
local TEST_FILE_PATH = TEST_DIR .. "test_datatable.bin"
local TEST_BLOCK_SIZE = 1024
local TEST_INTERVAL = 60 -- 1 minute interval
local TEST_RECORD_SIZE = 8 -- Dummy record size for testing (will be calculated by Columns)

-- Test configuration for DataTable
local TEST_CONFIG = {
    block_size = TEST_BLOCK_SIZE,
    columns = {
        { name = "time", type = "timestamp", interval = TEST_INTERVAL },
        { name = "value1", type = "number", precision = 0, signed = true }, -- Changed from 'int' to 'number'
        { name = "value2", type = "number", precision = 2, signed = false }, -- Changed from 'float' to 'number'
    }
}

-- Helper function to create a real Batch object with real Records
local function create_real_batch(columns_obj, start_time, count, value_func)
    local batch = Batch.new(columns_obj, false)
    for i = 0, count - 1 do
        local current_ts = start_time + i * columns_obj:get_interval()
        local values = { current_ts }
        if value_func then
            local extra_values = value_func(i, current_ts)
            for _, v in ipairs(extra_values) do
                table.insert(values, v)
            end
        else
            -- Default dummy values
            table.insert(values, i)
            table.insert(values, i + 0.1)
        end
        batch:add_record(Record.new(columns_obj, values))
    end
    return batch
end

-- Setup and Teardown
local function setup()
    os.execute("mkdir -p " .. TEST_DIR)
    os.execute("rm -f " .. TEST_FILE_PATH)
end

local function teardown()
    os.execute("rm -rf " .. TEST_DIR)
end

-- Test cases
function t_DataTable.test_new_uninitialized()
    setup()
    local dt = DataTable.new("test_table", TEST_CONFIG, TEST_FILE_PATH)
    assert(dt.name == "test_table", "Name mismatch")
    assert(dt.columns ~= nil, "Columns should be initialized")
    assert(dt.data_file ~= nil, "DataFile should be initialized")
    assert(not dt.initialized, "DataTable should not be initialized initially")
    assert(dt.interval == TEST_INTERVAL, "Interval mismatch")
    assert(dt.columns.record_size > 0, "Record size should be calculated")
    teardown()
end

function t_DataTable.test_new_initialized()
    setup()
    -- Create the file first to simulate an existing file
    local columns_obj = Columns.new({
        TimeCol.new("time", TEST_INTERVAL),
        NumberCol.new("value1", "number"), -- Changed from 'int' to 'number'
        NumberCol.new("value2", "number", 2) -- Changed from 'float' to 'number'
    })
    local df = DataFile.new(TEST_FILE_PATH, TEST_BLOCK_SIZE, columns_obj:get_interval(), columns_obj.record_size)
    df:create()

    local start_ts = 1000
    local count = 5
    local batch_to_write = create_real_batch(columns_obj, start_ts, count)
    df:write(batch_to_write)
    df:load() -- Load header

    local dt = DataTable.new("test_table", TEST_CONFIG, TEST_FILE_PATH)
    assert(dt.initialized, "DataTable should be initialized")
    assert(dt.data_file.start_time == start_ts, "DataFile start_time should be loaded")
    assert(dt.data_file.end_time == start_ts + (count - 1) * TEST_INTERVAL, "DataFile end_time should be loaded")
    teardown()
end

function t_DataTable.test_create()
    setup()
    local dt = DataTable.new("test_table", TEST_CONFIG, TEST_FILE_PATH)
    assert(not dt.initialized, "DataTable should not be initialized initially")
    dt:create()
    assert(dt.initialized, "DataTable should be initialized after create()")
    assert(dt.data_file:exist(), "DataFile should exist after create()")
    teardown()
end

function t_DataTable.test_get_stat_uninitialized()
    setup()
    local dt = DataTable.new("test_table", TEST_CONFIG, TEST_FILE_PATH)
    assert(dt:get_stat() == nil, "Stat should be nil for uninitialized table")
    teardown()
end

function t_DataTable.test_get_stat_initialized()
    setup()
    local dt = DataTable.new("test_table", TEST_CONFIG, TEST_FILE_PATH)
    dt:create()
    local stat = dt:get_stat()
    assert(stat ~= nil, "Stat should not be nil for initialized table")
    assert(stat.start_time == 0, "Stat start_time mismatch")
    assert(stat.end_time == 0, "Stat end_time mismatch")
    assert(stat.interval == TEST_INTERVAL, "Stat interval mismatch")
    assert(stat.file_size == Headers.header_length + TEST_BLOCK_SIZE, "Stat file_size mismatch")
    assert(stat.record_size == dt.columns.record_size, "Stat record_size mismatch")
    assert(stat.estimated_rows == 0, "Stat estimated_rows mismatch")
    teardown()
end

function t_DataTable.test_write_records_empty_batch()
    setup()
    local dt = DataTable.new("test_table", TEST_CONFIG, TEST_FILE_PATH)
    dt:create()
    local empty_batch = Batch.new(dt.columns, false)
    local written = dt:write_records(empty_batch)
    assert(written == 0, "Should write 0 records for empty batch")
    teardown()
end

function t_DataTable.test_write_records_first_batch()
    setup()
    local dt = DataTable.new("test_table", TEST_CONFIG, TEST_FILE_PATH)
    dt:create()

    local start_ts = 1000
    local count = 3
    local batch = create_real_batch(dt.columns, start_ts, count, function(i, ts) return {i + 1, (i + 1) + 0.1} end)

    local written = dt:write_records(batch)
    assert(written == count, "Should write " .. count .. " records")
    assert(dt.data_file.start_time == start_ts, "DataFile start_time mismatch")
    assert(dt.data_file.end_time == start_ts + (count - 1) * TEST_INTERVAL, "DataFile end_time mismatch")
    teardown()
end

function t_DataTable.test_write_records_with_gap()
    setup()
    local dt = DataTable.new("test_table", TEST_CONFIG, TEST_FILE_PATH)
    dt:create()

    local start_ts1 = 1000
    local count1 = 2
    local batch1 = create_real_batch(dt.columns, start_ts1, count1, function(i, ts) return {i + 1, (i + 1) + 0.1} end)
    dt:write_records(batch1)

    local end_ts1 = start_ts1 + (count1 - 1) * TEST_INTERVAL
    local start_ts2 = end_ts1 + TEST_INTERVAL * 3 -- Create a gap of 2 intervals
    local count2 = 2
    local batch2 = create_real_batch(dt.columns, start_ts2, count2, function(i, ts) return {i + 4, (i + 4) + 0.4} end)

    local written = dt:write_records(batch2)
    -- batch1 (2 records) + 2 blank records + batch2 (2 records) = 6 records total written to file
    -- DataFile:write returns batch:count() for the current batch, plus any blank records written.
    -- The `write_records` function in DataTable writes blanks then the actual batch.
    -- The return value is the sum of records written by DataFile.
    assert(written == count2 + 2, "Should write " .. (count2 + 2) .. " records (2 blanks + 2 actual)")
    assert(dt.data_file.start_time == start_ts1, "DataFile start_time mismatch after gap write")
    assert(dt.data_file.end_time == start_ts2 + (count2 - 1) * TEST_INTERVAL, "DataFile end_time mismatch after gap write")
    teardown()
end

function t_DataTable.test_query_records()
    setup()
    local dt = DataTable.new("test_table", TEST_CONFIG, TEST_FILE_PATH)
    dt:create()

    local start_ts = 1000
    local count = 5
    local batch_to_write = create_real_batch(dt.columns, start_ts, count, function(i, ts) return {i + 1, (i + 1) + 0.1} end)
    dt:write_records(batch_to_write)

    local query_start = start_ts + TEST_INTERVAL
    local query_end = start_ts + TEST_INTERVAL * 3
    local result_batch = dt:query_records(query_start, query_end, false)

    assert(result_batch:count() == 3, "Query records count mismatch")
    assert(result_batch:get_record(1):getTimestamp() == query_start, "Query records start_time mismatch")
    assert(result_batch:get_record(3):getTimestamp() == query_end, "Query records end_time mismatch")
    assert(result_batch:get_record(1):get_value_by_index(2) == 2, "Query record 1 value1 mismatch")
    assert(result_batch:get_record(1):get_value_by_index(3) == 2.1, "Query record 1 value2 mismatch")
    teardown()
end

function t_DataTable.test_query_group()
    setup()
    local dt = DataTable.new("test_table", TEST_CONFIG, TEST_FILE_PATH)
    dt:create()

    local start_ts = 1000
    local end_ts = start_ts + TEST_INTERVAL * 5
    local group_batch = dt:query_group(start_ts, end_ts, 10, false)

    assert(group_batch ~= nil, "GroupBatch should be returned")
    assert(group_batch.data_table == dt, "GroupBatch datatable mismatch")
    assert(group_batch.total_start == start_ts, "GroupBatch start_time mismatch")
    assert(group_batch.total_end == end_ts, "GroupBatch end_time mismatch")
    assert(group_batch.records_per_batch == 10, "GroupBatch records_per_batch mismatch")
    assert(group_batch.filter_nil == false, "GroupBatch filter_nil mismatch")
    teardown()
end

function t_DataTable.test_query_agg_tumbling()
    setup()
    local dt = DataTable.new("test_table", TEST_CONFIG, TEST_FILE_PATH)
    dt:create()

    local start_ts = 1000
    local count = 10
    local end_ts = start_ts + (count - 1) * TEST_INTERVAL
    -- Write some dummy data
    local batch_to_write = create_real_batch(dt.columns, start_ts, count, function(i, ts) return {i + 1, (i + 1) + 0.1} end)
    dt:write_records(batch_to_write)

    local agg_interval = TEST_INTERVAL * 3 -- Tumbling window of 3 intervals
    local mr_functions = {
        { column_id = 2, map = function(acc, val) return (acc or 0) + val end, reduce = function(acc) return acc end }, -- Sum of value1
        { column_id = 3, map = function(acc, val) return math.max(acc or 0, val) end, reduce = function(acc) return acc end }, -- Max of value2
    }

    local results = dt:query_agg_tumbling(start_ts, end_ts, agg_interval, mr_functions)

    assert(#results == 4, "Tumbling agg results count mismatch") -- 10 records / 3 interval = 3.33 -> 4 windows

    -- Expected results based on actual data:
    -- Records: (ts, value1, value2)
    -- (1000, 1, 1.1), (1060, 2, 2.1), (1120, 3, 3.1), (1180, 4, 4.1), (1240, 5, 5.1),
    -- (1300, 6, 6.1), (1360, 7, 7.1), (1420, 8, 8.1), (1480, 9, 9.1), (1540, 10, 10.1)

    -- Window 1 (1000-1120): (1000,1,1.1), (1060,2,2.1), (1120,3,3.1)
    -- Sum(value1): 1+2+3 = 6
    -- Max(value2): max(1.1,2.1,3.1) = 3.1
    assert(results[1][1] == 1000, "Tumbling agg result 1 timestamp mismatch")
    assert(results[1][2] == 6, "Tumbling agg result 1 sum mismatch")
    assert(results[1][3] == 3.1, "Tumbling agg result 1 max mismatch")

    -- Window 2 (1180-1300): (1180,4,4.1), (1240,5,5.1), (1300,6,6.1)
    -- Sum(value1): 4+5+6 = 15
    -- Max(value2): max(4.1,5.1,6.1) = 6.1
    assert(results[2][1] == 1180, "Tumbling agg result 2 timestamp mismatch")
    assert(results[2][2] == 15, "Tumbling agg result 2 sum mismatch")
    assert(results[2][3] == 6.1, "Tumbling agg result 2 max mismatch")

    -- Window 3 (1360-1480): (1360,7,7.1), (1420,8,8.1), (1480,9,9.1)
    -- Sum(value1): 7+8+9 = 24
    -- Max(value2): max(7.1,8.1,9.1) = 9.1
    assert(results[3][1] == 1360, "Tumbling agg result 3 timestamp mismatch")
    assert(results[3][2] == 24, "Tumbling agg result 3 sum mismatch")
    assert(results[3][3] == 9.1, "Tumbling agg result 3 max mismatch")

    -- Window 4 (1540-1540): (1540,10,10.1)
    -- Sum(value1): 10 = 10
    -- Max(value2): max(10.1) = 10.1
    assert(results[4][1] == 1540, "Tumbling agg result 4 timestamp mismatch")
    assert(results[4][2] == 10, "Tumbling agg result 4 sum mismatch")
    assert(results[4][3] == 10.1, "Tumbling agg result 4 max mismatch")

    teardown()
end

function t_DataTable.test_query_agg_sliding()
    setup()
    local dt = DataTable.new("test_table", TEST_CONFIG, TEST_FILE_PATH)
    dt:create()

    local start_ts = 1000
    local count = 10
    local end_ts = start_ts + (count - 1) * TEST_INTERVAL
    -- Write some dummy data
    local batch_to_write = create_real_batch(dt.columns, start_ts, count, function(i, ts) return {i + 1, (i + 1) + 0.1} end)
    dt:write_records(batch_to_write)

    local slidingSize = 3 -- Sliding window of 3 records
    local mr_functions = {
        { column_id = 2, map = function(acc, val) return (acc or 0) + val end, reduce = function(acc) return acc end }, -- Sum of value1
        { column_id = 3, map = function(acc, val) return math.max(acc or 0, val) end, reduce = function(acc) return acc end }, -- Max of value2
    }

    local results = dt:query_agg_sliding(start_ts, end_ts, slidingSize, mr_functions)

    assert(#results == 8, "Sliding agg results count mismatch") -- 10 records, sliding size 3 -> 10 - 3 + 1 = 8 results

    -- Records: (ts, value1, value2)
    -- (1000, 1, 1.1), (1060, 2, 2.1), (1120, 3, 3.1), (1180, 4, 4.1), (1240, 5, 5.1),
    -- (1300, 6, 6.1), (1360, 7, 7.1), (1420, 8, 8.1), (1480, 9, 9.1), (1540, 10, 10.1)

    -- Window 1 (records at 1000, 1060, 1120):
    -- value1: 1, 2, 3. Sum = 6
    -- value2: 1.1, 2.1, 3.1. Max = 3.1
    assert(results[1][1] == 1120, "Sliding agg result 1 timestamp mismatch")
    assert(results[1][2] == 6, "Sliding agg result 1 sum mismatch")
    assert(results[1][3] == 3.1, "Sliding agg result 1 max mismatch")

    -- Window 2 (records at 1060, 1120, 1180):
    -- value1: 2, 3, 4. Sum = 9
    -- value2: 2.1, 3.1, 4.1. Max = 4.1
    assert(results[2][1] == 1180, "Sliding agg result 2 timestamp mismatch")
    assert(results[2][2] == 9, "Sliding agg result 2 sum mismatch")
    assert(results[2][3] == 4.1, "Sliding agg result 2 max mismatch")

    -- Window 3 (records at 1120, 1180, 1240):
    -- value1: 3, 4, 5. Sum = 12
    -- value2: 3.1, 4.1, 5.1. Max = 5.1
    assert(results[3][1] == 1240, "Sliding agg result 3 timestamp mismatch")
    assert(results[3][2] == 12, "Sliding agg result 3 sum mismatch")
    assert(results[3][3] == 5.1, "Sliding agg result 3 max mismatch")

    -- Window 8 (records at 1420, 1480, 1540):
    -- value1: 8, 9, 10. Sum = 27
    -- value2: 8.1, 9.1, 10.1. Max = 10.1
    assert(results[8][1] == 1540, "Sliding agg result 8 timestamp mismatch")
    assert(results[8][2] == 27, "Sliding agg result 8 sum mismatch")
    assert(results[8][3] == 10.1, "Sliding agg result 8 max mismatch")

    teardown()
end

-- New tests for invalid column configurations
function t_DataTable.test_new_invalid_column_config()
    setup()

    -- Test: Missing 'name' in a column
    local config_missing_name = {
        block_size = TEST_BLOCK_SIZE,
        columns = {
            { name = "time", type = "timestamp", interval = TEST_INTERVAL },
            { type = "number", precision = 0, signed = true }, -- Missing name, type changed to 'number'
        }
    }
    TestTools.assertErrorMsgContains("Column 2: 'name' is missing.", function() DataTable.new("test_table", config_missing_name, TEST_FILE_PATH) end)

    -- Test: Missing 'type' in a column
    local config_missing_type = {
        block_size = TEST_BLOCK_SIZE,
        columns = {
            { name = "time", type = "timestamp", interval = TEST_INTERVAL },
            { name = "value1", precision = 0, signed = true }, -- Missing type
        }
    }
    TestTools.assertErrorMsgContains(" 'type' is missing.", function() DataTable.new("test_table", config_missing_type, TEST_FILE_PATH) end)

    -- Test: First column not 'timestamp'
    local config_first_not_timestamp = {
        block_size = TEST_BLOCK_SIZE,
        columns = {
            { name = "value1", type = "number", precision = 0, signed = true }, -- First column not timestamp, type changed to 'number'
            { name = "time", type = "timestamp", interval = TEST_INTERVAL },
        }
    }
    TestTools.assertErrorMsgContains(" must be 'timestamp'.", function() DataTable.new("test_table", config_first_not_timestamp, TEST_FILE_PATH) end)

    -- Test: Timestamp column with invalid interval
    local config_invalid_timestamp_interval = {
        block_size = TEST_BLOCK_SIZE,
        columns = {
            { name = "time", type = "timestamp", interval = 0 }, -- Invalid interval
            { name = "value1", type = "number", precision = 0, signed = true }, -- Type changed to 'number'
        }
    }
    TestTools.assertErrorMsgContains("'interval' must be a positive number.", function() DataTable.new("test_table", config_invalid_timestamp_interval, TEST_FILE_PATH) end)

    -- Test: Precision exceeds max_precision for a number type
    local config_exceed_precision = {
        block_size = TEST_BLOCK_SIZE,
        columns = {
            { name = "time", type = "timestamp", interval = TEST_INTERVAL },
            { name = "value1", type = "number", precision = 100, signed = true }, -- Exceeds max_precision for 'number', type changed to 'number'
        }
    }
    TestTools.assertErrorMsgContains(" exceeds its max_precision", function() DataTable.new("test_table", config_exceed_precision, TEST_FILE_PATH) end)

    -- Test: Too few columns
    local config_too_few_columns = {
        block_size = TEST_BLOCK_SIZE,
        columns = {
            { name = "time", type = "timestamp", interval = TEST_INTERVAL },
        }
    }
    TestTools.assertErrorMsgContains("Columns.new: 'columns_list' must contain at least 2 columns.", function() DataTable.new("test_table", config_too_few_columns, TEST_FILE_PATH) end)

    -- Test: Too many columns (more than 32)
    local many_columns = {}
    table.insert(many_columns, { name = "time", type = "timestamp", interval = TEST_INTERVAL })
    for i = 1, 32 do -- 32 + 1 = 33 columns
        table.insert(many_columns, { name = "value" .. i, type = "number" }) -- Type changed to 'number'
    end
    local config_too_many_columns = {
        block_size = TEST_BLOCK_SIZE,
        columns = many_columns
    }
    TestTools.assertErrorMsgContains("Columns.new: 'columns_list' size cannot be greater than 32.", function() DataTable.new("test_table", config_too_many_columns, TEST_FILE_PATH) end)

    teardown()
end

return t_DataTable