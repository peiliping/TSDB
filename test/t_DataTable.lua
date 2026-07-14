local TestTools = require("test.TestTools")

local DataFile = require("db.DataFile")
local DataTable = require("db.DataTable")

local Record = require("record.Record")
local Batch = require("record.Batch")

local TableConfig = require("conf.TableConfig")
local ConfigToColumns = require("conf.ConfigToColumns")

local test_case = {}

local TEST_DIR = "./test_data/"
local TEST_FILE_PATH = TEST_DIR .. "test_datatable.bin"
local TEST_BLOCK_SIZE = 1024
local TEST_INTERVAL = 60

local TEST_TABLE_CONFIG_COLUMNS = {
    { name = "time", type = "timestamp", interval = TEST_INTERVAL },
    { name = "value1", type = "number", precision = 0, signed = true },
    { name = "value2", type = "number", precision = 2, signed = false },
}
local TEST_TABLE_CONFIG = TableConfig.new(TEST_TABLE_CONFIG_COLUMNS, TEST_BLOCK_SIZE)

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
            table.insert(values, i)
            table.insert(values, i + 0.1)
        end
        batch:add_record(Record.new(columns_obj, values))
    end
    return batch
end

local function setup()
    os.execute("mkdir -p " .. TEST_DIR)
    os.execute("rm -f " .. TEST_FILE_PATH)
end

local function teardown()
    os.execute("rm -rf " .. TEST_DIR)
end

function test_case.test_new_uninitialized()
    setup()
    local dt = DataTable.new("test_table", TEST_TABLE_CONFIG, TEST_FILE_PATH)
    assert(dt.name == "test_table", "Name mismatch")
    assert(dt.columns ~= nil, "Columns should be initialized")
    assert(dt.data_file ~= nil, "DataFile should be initialized")
    assert(not dt.initialized, "DataTable should not be initialized initially")
    assert(dt.interval == TEST_INTERVAL, "Interval mismatch")
    assert(dt.columns.record_size > 0, "Record size should be calculated")
    teardown()
end

function test_case.test_new_initialized()
    setup()
    -- 使用 ConfigToColumns.convert 从 TEST_TABLE_CONFIG 创建 columns_obj
    local columns_obj = ConfigToColumns.convert(TEST_TABLE_CONFIG)
    local df = DataFile.new(TEST_FILE_PATH, TEST_BLOCK_SIZE, columns_obj:get_interval(), columns_obj.record_size)
    df:create()

    local start_ts = 1200
    local count = 5
    local batch_to_write = create_real_batch(columns_obj, start_ts, count)
    df:write(batch_to_write)
    df:load()

    local dt = DataTable.new("test_table", TEST_TABLE_CONFIG, TEST_FILE_PATH)
    assert(dt.initialized, "DataTable should be initialized")
    assert(dt.data_file.start_time == start_ts, "DataFile start_time should be loaded")
    assert(dt.data_file.end_time == start_ts + (count - 1) * TEST_INTERVAL, "DataFile end_time should be loaded")
    teardown()
end

function test_case.test_create()
    setup()
    local dt = DataTable.new("test_table", TEST_TABLE_CONFIG, TEST_FILE_PATH)
    assert(not dt.initialized, "DataTable should not be initialized initially")
    dt:create()
    assert(dt.initialized, "DataTable should be initialized after create()")
    assert(dt.data_file:exist(), "DataFile should exist after create()")
    teardown()
end

function test_case.test_get_stat_uninitialized()
    setup()
    local dt = DataTable.new("test_table", TEST_TABLE_CONFIG, TEST_FILE_PATH)
    assert(dt:get_stat() == nil, "Stat should be nil for uninitialized table")
    teardown()
end

function test_case.test_write_records_empty_batch()
    setup()
    local dt = DataTable.new("test_table", TEST_TABLE_CONFIG, TEST_FILE_PATH)
    dt:create()
    local empty_batch = Batch.new(dt.columns, false)
    local written = dt:write_records(empty_batch)
    assert(written == 0, "Should write 0 records for empty batch")
    teardown()
end

function test_case.test_write_records_first_batch()
    setup()
    local dt = DataTable.new("test_table", TEST_TABLE_CONFIG, TEST_FILE_PATH)
    dt:create()

    local start_ts = 1200
    local count = 3
    local batch = create_real_batch(dt.columns, start_ts, count, function(i, ts) return {i + 1, (i + 1) + 0.1} end)

    local written = dt:write_records(batch)
    assert(written == count, "Should write " .. count .. " records")
    assert(dt.data_file.start_time == start_ts, "DataFile start_time mismatch")
    assert(dt.data_file.end_time == start_ts + (count - 1) * TEST_INTERVAL, "DataFile end_time mismatch")
    teardown()
end

function test_case.test_write_records_with_gap()
    setup()
    local dt = DataTable.new("test_table", TEST_TABLE_CONFIG, TEST_FILE_PATH)
    dt:create()

    local start_ts1 = 1200
    local count1 = 2
    local batch1 = create_real_batch(dt.columns, start_ts1, count1, function(i, ts) return {i + 1, (i + 1) + 0.1} end)
    dt:write_records(batch1)

    local end_ts1 = start_ts1 + (count1 - 1) * TEST_INTERVAL
    local start_ts2 = end_ts1 + TEST_INTERVAL * 3
    local count2 = 2
    local batch2 = create_real_batch(dt.columns, start_ts2, count2, function(i, ts) return {i + 4, (i + 4) + 0.4} end)

    local written = dt:write_records(batch2)
    assert(written == count2 + 2, "Should write " .. (count2 + 2) .. " records (2 blanks + 2 actual)")
    assert(dt.data_file.start_time == start_ts1, "DataFile start_time mismatch after gap write")
    assert(dt.data_file.end_time == start_ts2 + (count2 - 1) * TEST_INTERVAL, "DataFile end_time mismatch after gap write")
    teardown()
end

function test_case.test_query_records()
    setup()
    local dt = DataTable.new("test_table", TEST_TABLE_CONFIG, TEST_FILE_PATH)
    dt:create()

    local start_ts = 1200
    local count = 5
    local batch_to_write = create_real_batch(dt.columns, start_ts, count, function(i, ts) return {i + 1, (i + 1) + 0.1} end)
    dt:write_records(batch_to_write)

    local query_start = start_ts + TEST_INTERVAL
    local query_end = start_ts + TEST_INTERVAL * 3
    local result_batch = dt:query_records(query_start, query_end, false)

    assert(result_batch:count() == 3, "Query records count mismatch")
    assert(result_batch:get_record(1):get_timestamp() == query_start, "Query records start_time mismatch")
    assert(result_batch:get_record(3):get_timestamp() == query_end, "Query records end_time mismatch")
    assert(result_batch:get_record(1):get_value_by_index(2) == 2, "Query record 1 value1 mismatch")
    assert(math.abs(result_batch:get_record(1):get_value_by_index(3) - 2.1) < 0.001, "Query record 1 value2 mismatch")
    teardown()
end

function test_case.test_query_group()
    setup()
    local dt = DataTable.new("test_table", TEST_TABLE_CONFIG, TEST_FILE_PATH)
    dt:create()

    local start_ts = 1200
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

function test_case.test_query_agg_tumbling()
    setup()
    local dt = DataTable.new("test_table", TEST_TABLE_CONFIG, TEST_FILE_PATH)
    dt:create()

    local start_ts = 1200
    local count = 10
    local end_ts = start_ts + (count - 1) * TEST_INTERVAL
    local batch_to_write = create_real_batch(dt.columns, start_ts, count, function(i, ts) return {i + 1, (i + 1) + 0.1} end)
    dt:write_records(batch_to_write)

    local agg_interval = TEST_INTERVAL * 3
    local mr_functions = {
        { column_id = 2, result_id = 2, result_size = 1, map = function(acc, val) return (acc or 0) + val end, reduce = function(acc) return acc end },
        { column_id = 3, result_id = 3, result_size = 1, map = function(acc, val) return math.max(acc or 0, val) end, reduce = function(acc) return acc end },
    }
    local cb = function(rb)
        assert(rb:size() == 4, "Tumbling agg results count mismatch")

        assert(rb:get(1)[1] == 1080, "Tumbling agg result 1 timestamp mismatch")
        assert(rb:get(1)[2] == 1, "Tumbling agg result 1 sum mismatch")
        assert(math.abs(rb:get(1)[3] - 1.1) < 0.001, "Tumbling agg result 1 max mismatch")

        assert(rb:get(2)[1] == 1260, "Tumbling agg result 2 timestamp mismatch")
        assert(rb:get(2)[2] == 9, "Tumbling agg result 2 sum mismatch")
        assert(math.abs(rb:get(2)[3] - 4.1) < 0.001, "Tumbling agg result 2 max mismatch")

        assert(rb:get(3)[1] == 1440, "Tumbling agg result 3 timestamp mismatch")
        assert(rb:get(3)[2] == 18, "Tumbling agg result 3 sum mismatch")
        assert(math.abs(rb:get(3)[3] - 7.1) < 0.001, "Tumbling agg result 3 max mismatch")

        assert(rb:get(4)[1] == 1620, "Tumbling agg result 4 timestamp mismatch")
        assert(rb:get(4)[2] == 27, "Tumbling agg result 4 sum mismatch")
        assert(math.abs(rb:get(4)[3] - 10.1) < 0.001, "Tumbling agg result 4 max mismatch")

    end
    dt:query_agg_tumbling(start_ts, end_ts, agg_interval, mr_functions, cb)
    teardown()
end

function test_case.test_query_agg_sliding()
    setup()
    local dt = DataTable.new("test_table", TEST_TABLE_CONFIG, TEST_FILE_PATH)
    dt:create()

    local start_ts = 1200
    local count = 10
    local end_ts = start_ts + (count - 1) * TEST_INTERVAL
    local batch_to_write = create_real_batch(dt.columns, start_ts, count, function(i, ts) return {i + 1, (i + 1) + 0.1} end)
    dt:write_records(batch_to_write)

    local sliding_size = 3
    local mr_functions = {
        { column_id = 2, result_id = 2, result_size = 1, map = function(acc, val) return (acc or 0) + val end, reduce = function(acc) return acc end },
        { column_id = 3, result_id = 3, result_size = 1, map = function(acc, val) return math.max(acc or 0, val) end, reduce = function(acc) return acc end },
    }
    local cb = function(rb)
        assert(rb:size() == 8, "Sliding agg results count mismatch")

        assert(rb:get(1)[1] == 1320, "Sliding agg result 1 timestamp mismatch")
        assert(rb:get(1)[2] == 6, "Sliding agg result 1 sum mismatch")
        assert(math.abs(rb:get(1)[3] - 3.1) < 0.001, "Sliding agg result 1 max mismatch")

        assert(rb:get(2)[1] == 1380, "Sliding agg result 2 timestamp mismatch")
        assert(rb:get(2)[2] == 9, "Sliding agg result 2 sum mismatch")
        assert(math.abs(rb:get(2)[3] - 4.1) < 0.001, "Sliding agg result 2 max mismatch")

        assert(rb:get(3)[1] == 1440, "Sliding agg result 3 timestamp mismatch")
        assert(rb:get(3)[2] == 12, "Sliding agg result 3 sum mismatch")
        assert(math.abs(rb:get(3)[3] - 5.1) < 0.001, "Sliding agg result 3 max mismatch")

        assert(rb:get(8)[1] == 1740, "Sliding agg result 8 timestamp mismatch")
        assert(rb:get(8)[2] == 27, "Sliding agg result 8 sum mismatch")
        assert(math.abs(rb:get(8)[3] - 10.1) < 0.001, "Sliding agg result 8 max mismatch")
    end
    dt:query_agg_sliding(start_ts, end_ts, sliding_size, mr_functions, cb)
    teardown()
end

function test_case.test_new_invalid_column_config()
    setup()

    -- 注意：这里的配置对象直接传递给 DataTable.new，它会内部调用 ConfigToColumns.convert 和 Columns.new
    -- 因此，错误消息的来源是这些内部模块

    local config_missing_name = TableConfig.new({
        { name = "time", type = "timestamp", interval = TEST_INTERVAL },
        { type = "number", precision = 0, signed = true }, -- 缺少 name
    }, TEST_BLOCK_SIZE)
    TestTools.assert_error_msg_contains("Column 2: 'name' is missing.", function() DataTable.new("test_table", config_missing_name, TEST_FILE_PATH) end)

    local config_missing_type = TableConfig.new({
        { name = "time", type = "timestamp", interval = TEST_INTERVAL },
        { name = "value1", precision = 0, signed = true }, -- 缺少 type
    }, TEST_BLOCK_SIZE)
    TestTools.assert_error_msg_contains(" 'type' is missing.", function() DataTable.new("test_table", config_missing_type, TEST_FILE_PATH) end)

    local config_first_not_timestamp = TableConfig.new({
        { name = "value1", type = "number", precision = 0, signed = true },
        { name = "time", type = "timestamp", interval = TEST_INTERVAL },
    }, TEST_BLOCK_SIZE)
    TestTools.assert_error_msg_contains(" must be 'timestamp'.", function() DataTable.new("test_table", config_first_not_timestamp, TEST_FILE_PATH) end)

    local config_invalid_timestamp_interval = TableConfig.new({
        { name = "time", type = "timestamp", interval = 0 }, -- 无效的 interval
        { name = "value1", type = "number", precision = 0, signed = true },
    }, TEST_BLOCK_SIZE)
    TestTools.assert_error_msg_contains("'interval' must be a positive number.", function() DataTable.new("test_table", config_invalid_timestamp_interval, TEST_FILE_PATH) end)

    local config_exceed_precision = TableConfig.new({
        { name = "time", type = "timestamp", interval = TEST_INTERVAL },
        { name = "value1", type = "number", precision = 100, signed = true }, -- 精度超出范围
    }, TEST_BLOCK_SIZE)
    TestTools.assert_error_msg_contains(" exceeds its max_precision", function() DataTable.new("test_table", config_exceed_precision, TEST_FILE_PATH) end)

    local config_too_few_columns = TableConfig.new({
        { name = "time", type = "timestamp", interval = TEST_INTERVAL },
    }, TEST_BLOCK_SIZE)
    TestTools.assert_error_msg_contains("Columns.new: 'columns_list' must contain at least 2 columns.", function() DataTable.new("test_table", config_too_few_columns, TEST_FILE_PATH) end)

    local many_columns = {}
    table.insert(many_columns, { name = "time", type = "timestamp", interval = TEST_INTERVAL })
    for i = 1, 32 do
        table.insert(many_columns, { name = "value" .. i, type = "number" })
    end
    local config_too_many_columns = TableConfig.new(many_columns, TEST_BLOCK_SIZE)
    TestTools.assert_error_msg_contains("Columns.new: 'columns_list' size cannot be greater than 32.", function() DataTable.new("test_table", config_too_many_columns, TEST_FILE_PATH) end)

    teardown()
end

return test_case