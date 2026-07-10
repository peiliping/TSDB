local Batch = require("record.Batch")
local Record = require("record.Record")
local BitTools = require("tools.BitTools")
local TestTools = require("test.TestTools")

local Columns = require("record.col.Columns")
local NumberCol = require("record.col.NumberCol")
local TimeCol = require("record.col.TimeCol")

local test_case = {}

function test_case.test_Batch_new()
    local interval = 60
    local col1 = TimeCol.new("timestamp", interval)
    local col2 = NumberCol.new("value", "number", 2, false)

    local real_cols = Columns.new({ col1, col2 })

    local batch = Batch.new(real_cols)
    assert(batch ~= nil, "Batch should be created")
    assert(batch.columns == real_cols, "Columns should match")
    assert(#batch.datas == 0, "datas should be empty")
    assert(#batch.nil_flags == 0, "nil_flags should be empty")
    assert(batch.filter_nil == false, "filter_nil should be false by default")

    local filtered_batch = Batch.new(real_cols, true)
    assert(filtered_batch.filter_nil == true, "filter_nil should be true when specified")

    TestTools.assertErrorMsgContains("'columns' must be a table.", function()
        Batch.new(nil)
    end)
end

function test_case.test_Batch_add_records()
    local interval = 60
    local col1 = TimeCol.new("timestamp", interval)
    local col2 = NumberCol.new("value", "number", 2, false)
    local real_cols = Columns.new({ col1, col2 })

    local batch = Batch.new(real_cols)

    local fixed_ts_base = 1200 * interval

    local ts1 = fixed_ts_base
    batch:add({ ts1, 10.5 })
    assert(batch:count() == 1, "Batch count should be 1")
    assert(batch:start_time() == ts1, "Start time should be ts1")
    assert(batch:end_time() == ts1, "End time should be ts1")

    local ts2 = ts1 + interval
    batch:add({ ts2, 11.0 })
    assert(batch:count() == 2, "Batch count should be 2")
    assert(batch:start_time() == ts1, "Start time should remain ts1")
    assert(batch:end_time() == ts2, "End time should be ts2")

    local ts3 = ts2 + interval * 3
    batch:add({ ts3, 12.0 })
    assert(batch:count() == 5, "Batch count should be 5 (2 original + 2 nil + 1 new)")
    assert(batch:end_time() == ts3, "End time should be ts3")

    local record_ts2_plus_interval = batch:get_record(3)
    assert(record_ts2_plus_interval:get_timestamp() == ts2 + interval, "Filled record timestamp incorrect")
    assert(record_ts2_plus_interval:is_nil_record(), "Should be a nil record")
    local record_ts2_plus_2_interval = batch:get_record(4)
    assert(record_ts2_plus_2_interval:get_timestamp() == ts2 + 2 * interval, "Filled record timestamp incorrect")
    assert(record_ts2_plus_2_interval:is_nil_record(), "Should be a nil record")

    local ts_out_of_order = ts1 + interval / 2
    TestTools.assertErrorMsgContains("Data Time not match interval.", function()
        batch:add({ ts_out_of_order, 9.0 })
    end)

    local filtered_batch = Batch.new(real_cols, true)
    local nil_record_flags = real_cols.nil_record_flags
    filtered_batch:add({ fixed_ts_base, nil }, nil_record_flags)
    assert(filtered_batch:count() == 0, "Nil record should be filtered out")

    filtered_batch:add({ fixed_ts_base + interval, 100.0 })
    assert(filtered_batch:count() == 1, "Non-nil record should be added")
end

function test_case.test_Batch_add_multiple_records()
    local interval = 60
    local col1 = TimeCol.new("timestamp", interval)
    local col2 = NumberCol.new("value", "number", 2, false)
    local real_cols = Columns.new({ col1, col2 })

    local batch = Batch.new(real_cols)

    local ts_base = 1200 * interval

    local records_to_add = {
        Record.new(real_cols, { ts_base, 1.1 }),
        Record.new(real_cols, { ts_base + interval, 2.2 }),
        Record.new(real_cols, { ts_base + 3 * interval, 3.3 }),
    }

    batch:add_records(records_to_add)
    assert(batch:count() == 4, "Batch count should be 4 (3 original + 1 nil)")
    assert(batch:start_time() == ts_base, "Start time incorrect")
    assert(batch:end_time() == ts_base + 3 * interval, "End time incorrect")

    local filled_record = batch:get_record(3)
    assert(filled_record:get_timestamp() == ts_base + 2 * interval, "Filled record timestamp incorrect")
    assert(filled_record:is_nil_record(), "Filled record should be nil")
end

function test_case.test_Batch_get_record()
    local interval = 60
    local col1 = TimeCol.new("timestamp", interval)
    local col2 = NumberCol.new("value", "number", 2, false)
    local real_cols = Columns.new({ col1, col2 })

    local batch = Batch.new(real_cols)
    local ts = 1200 * interval
    batch:add({ ts, 10.5 })
    batch:add({ ts + interval, 11.0 })

    local record1 = batch:get_record(1)
    assert(record1:get_timestamp() == ts, "get_record(1) timestamp incorrect")
    assert(math.abs(record1:get_value("value") - 10.5) < 0.001, "get_record(1) value incorrect")

    local record2 = batch:get_record(2)
    assert(record2:get_timestamp() == ts + interval, "get_record(2) timestamp incorrect")
    assert(math.abs(record2:get_value("value") - 11.0) < 0.001, "get_record(2) value incorrect")

    TestTools.assertErrorMsgContains("Index 0 is out of bounds", function()
        batch:get_record(0)
    end)
    TestTools.assertErrorMsgContains("Index 3 is out of bounds", function()
        batch:get_record(3)
    end)
end

function test_case.test_Batch_binary_serialization()
    local interval = 60
    local col1 = TimeCol.new("timestamp", interval)
    local col2 = NumberCol.new("value", "number", 2, false)
    local col3 = NumberCol.new("status", "number", 0, false)
    local real_cols = Columns.new({ col1, col2, col3 })

    local original_batch = Batch.new(real_cols)
    local ts_base = 1200 * interval

    original_batch:add({ ts_base, 10.1, 1 })
    original_batch:add({ ts_base + interval, nil, 0 })
    original_batch:add({ ts_base + 3 * interval, 12.3, 1 })

    local binary_data = original_batch:toBinary()
    assert(binary_data ~= nil and #binary_data > 0, "Binary data should not be empty")

    local decoded_batch = Batch.new(real_cols)
    decoded_batch:fromBinary(binary_data)

    assert(decoded_batch:count() == original_batch:count(), "Decoded batch count should match original")
    assert(decoded_batch:start_time() == original_batch:start_time(), "Decoded start time should match")
    assert(decoded_batch:end_time() == original_batch:end_time(), "Decoded end time should match")

    for i = 1, original_batch:count() do
        local original_record = original_batch:get_record(i)
        local decoded_record = decoded_batch:get_record(i)

        assert(original_record:get_timestamp() == decoded_record:get_timestamp(), "Record " .. i .. " timestamp mismatch")
        assert(original_record:get_value("status") == decoded_record:get_value("status"), "Record " .. i .. " status mismatch")
        assert(original_record.nil_flags == decoded_record.nil_flags, "Record " .. i .. " nil_flags mismatch")

        -- Modified assertion to handle nil values for 'value' column
        local original_value = original_record:get_value("value")
        local decoded_value = decoded_record:get_value("value")

        if original_value == nil and decoded_value == nil then
            -- Both are nil, which is a match
            assert(true, "Record " .. i .. " value: both nil")
        elseif original_value ~= nil and decoded_value ~= nil then
            -- Both are non-nil, compare with tolerance
            assert(math.abs(original_value - decoded_value) < 0.001, "Record " .. i .. " value mismatch (non-nil)")
        else
            -- One is nil and the other is not, which is a mismatch
            assert(false, "Record " .. i .. " value mismatch (one nil, one not)")
        end
    end
end

return test_case