local Batch = require("record.Batch")
local Record = require("record.Record")
local BitTools = require("tools.BitTools")
local TestTools = require("test.TestTools")

local test_case = {}

-- Mock Column object (reused from t_BinaryTools.lua)
local MockColumn = {}
function MockColumn:new(name, type_name, format, size) -- Added type_name
    local o = {
        name = name,
        type_name = type_name, -- Added
        format = format,
        size = size,
    }
    setmetatable(o, self)
    self.__index = self
    return o
end

function MockColumn:pack_value(value)
    if self.type_name == "timestamp" then -- Use type_name for packing logic
        return value or 0
    elseif self.type_name == "number" then
        return value or 0.0
    else
        return value
    end
end

function MockColumn:unpack_value(value)
    return value
end

-- Mock Columns object for Batch tests (extended from t_Record.lua)
local MockColumns = {}
function MockColumns:new(cols_definition, interval)
    local o = {
        cols = cols_definition,
        name_to_index = {},
        nil_record_flags = 0,
        format_string = "I", -- Assuming nil_flags is an unsigned int
        interval = interval or 60, -- Default interval
        record_size = 0, -- Calculate record_size
    }
    setmetatable(o, self)
    self.__index = self

    for i, col in ipairs(cols_definition) do
        o.name_to_index[col.name] = i
        o.format_string = o.format_string .. col.format
        o.record_size = o.record_size + col.size -- Accumulate record size
    end
    o.nil_record_flags = BitTools.calculate_nil_record_flags(#cols_definition)
    return o
end

function MockColumns:count()
    return #self.cols
end

function MockColumns:get_index_by_name(name)
    local index = self.name_to_index[name]
    if not index then
        error("Column not found: " .. name)
    end
    return index
end

function MockColumns:get_by_index(index) -- Added get_by_index
    local col = self.cols[index]
    if not col then
        error("Column not found at index: " .. tostring(index))
    end
    return col
end

function MockColumns:get_interval()
    return self.interval
end

-- Test for Batch.new
function test_case.test_Batch_new()
    local col1 = MockColumn:new("timestamp", "timestamp", "I4", 4) -- Added type_name
    local col2 = MockColumn:new("value", "number", "f", 4) -- Added type_name
    local mock_cols = MockColumns:new({ col1, col2 })

    local batch = Batch.new(mock_cols)
    assert(batch ~= nil, "Batch should be created")
    assert(batch.columns == mock_cols, "Columns should match")
    assert(#batch.datas == 0, "datas should be empty")
    assert(#batch.nil_flags == 0, "nil_flags should be empty")
    assert(batch.filter_nil == false, "filter_nil should be false by default")

    local filtered_batch = Batch.new(mock_cols, true)
    assert(filtered_batch.filter_nil == true, "filter_nil should be true when specified")

    TestTools.assertErrorMsgContains("'columns' must be a table.", function()
        Batch.new(nil)
    end)
end

-- Test for Batch:add and Batch:add_record
function test_case.test_Batch_add_records()
    local col1 = MockColumn:new("timestamp", "timestamp", "I4", 4)
    local col2 = MockColumn:new("value", "number", "f", 4)
    local mock_cols = MockColumns:new({ col1, col2 }, 60) -- Interval 60 seconds

    local batch = Batch.new(mock_cols)

    local fixed_ts_base = 1200 -- Fixed timestamp base

    -- Add first record
    local ts1 = fixed_ts_base
    batch:add({ ts1, 10.5 })
    assert(batch:count() == 1, "Batch count should be 1")
    assert(batch:start_time() == ts1, "Start time should be ts1")
    assert(batch:end_time() == ts1, "End time should be ts1")

    -- Add second record (in order)
    local ts2 = ts1 + 60
    batch:add({ ts2, 11.0 })
    assert(batch:count() == 2, "Batch count should be 2")
    assert(batch:start_time() == ts1, "Start time should remain ts1")
    assert(batch:end_time() == ts2, "End time should be ts2")

    -- Add record with a gap, should fill nil records
    local ts3 = ts2 + 60 * 3 -- 3 intervals later, so 2 nil records should be filled
    batch:add({ ts3, 12.0 })
    assert(batch:count() == 5, "Batch count should be 5 (2 original + 2 nil + 1 new)")
    assert(batch:end_time() == ts3, "End time should be ts3")
    -- Check filled nil records
    local record_ts2_plus_60 = batch:get_record(3)
    assert(record_ts2_plus_60:getTimestamp() == ts2 + 60, "Filled record timestamp incorrect")
    assert(record_ts2_plus_60:is_nil_record(), "Should be a nil record")
    local record_ts2_plus_120 = batch:get_record(4)
    assert(record_ts2_plus_120:getTimestamp() == ts2 + 120, "Filled record timestamp incorrect")
    assert(record_ts2_plus_120:is_nil_record(), "Should be a nil record")

    -- Test adding a record out of order
    local ts_out_of_order = ts1 + 30
    TestTools.assertErrorMsgContains("Data Time not match interval.", function()
        batch:add({ ts_out_of_order, 9.0 })
    end)

    -- Test filter_nil = true
    local filtered_batch = Batch.new(mock_cols, true)
    local nil_record_flags = mock_cols.nil_record_flags
    filtered_batch:add({ fixed_ts_base, nil }, nil_record_flags) -- Add a nil record
    assert(filtered_batch:count() == 0, "Nil record should be filtered out")

    filtered_batch:add({ fixed_ts_base + 60, 100.0 })
    assert(filtered_batch:count() == 1, "Non-nil record should be added")
end

-- Test for Batch:add_records
function test_case.test_Batch_add_multiple_records()
    local col1 = MockColumn:new("timestamp", "timestamp", "I4", 4)
    local col2 = MockColumn:new("value", "number", "f", 4)
    local mock_cols = MockColumns:new({ col1, col2 }, 60)

    local batch = Batch.new(mock_cols)
    local ts_base = 1200 -- Fixed timestamp base

    local records_to_add = {
        Record.new(mock_cols, { ts_base, 1.1 }),
        Record.new(mock_cols, { ts_base + 60, 2.2 }),
        Record.new(mock_cols, { ts_base + 180, 3.3 }), -- Gap of 1 interval
    }

    batch:add_records(records_to_add)
    assert(batch:count() == 4, "Batch count should be 4 (3 original + 1 nil)")
    assert(batch:start_time() == ts_base, "Start time incorrect")
    assert(batch:end_time() == ts_base + 180, "End time incorrect")

    -- Check the nil record that was filled
    local filled_record = batch:get_record(3)
    assert(filled_record:getTimestamp() == ts_base + 120, "Filled record timestamp incorrect")
    assert(filled_record:is_nil_record(), "Filled record should be nil")
end

-- Test for Batch:get_record
function test_case.test_Batch_get_record()
    local col1 = MockColumn:new("timestamp", "timestamp", "I4", 4)
    local col2 = MockColumn:new("value", "number", "f", 4)
    local mock_cols = MockColumns:new({ col1, col2 })

    local batch = Batch.new(mock_cols)
    local ts = 1200 -- Fixed timestamp base
    batch:add({ ts, 10.5 })
    batch:add({ ts + 60, 11.0 })

    local record1 = batch:get_record(1)
    assert(record1:getTimestamp() == ts, "get_record(1) timestamp incorrect")
    assert(record1:get_value("value") == 10.5, "get_record(1) value incorrect")

    local record2 = batch:get_record(2)
    assert(record2:getTimestamp() == ts + 60, "get_record(2) timestamp incorrect")
    assert(record2:get_value("value") == 11.0, "get_record(2) value incorrect")

    TestTools.assertErrorMsgContains("Index 0 is out of bounds", function()
        batch:get_record(0)
    end)
    TestTools.assertErrorMsgContains("Index 3 is out of bounds", function()
        batch:get_record(3)
    end)
end

-- Test for Batch:toBinary and Batch:fromBinary
function test_case.test_Batch_binary_serialization()
    local col1 = MockColumn:new("timestamp", "timestamp", "I4", 4)
    local col2 = MockColumn:new("value", "number", "f", 4)
    local col3 = MockColumn:new("status", "number", "I4", 4)
    local mock_cols = MockColumns:new({ col1, col2, col3 }, 60)

    local original_batch = Batch.new(mock_cols)
    local ts_base = 1200 -- Fixed timestamp base
    original_batch:add({ ts_base, 10.1, 1 })
    original_batch:add({ ts_base + 60, nil, 0 }) -- Nil value
    original_batch:add({ ts_base + 180, 12.3, 1 }) -- Gap, should fill a nil record

    local binary_data = original_batch:toBinary()
    assert(binary_data ~= nil and #binary_data > 0, "Binary data should not be empty")

    local decoded_batch = Batch.new(mock_cols)
    decoded_batch:fromBinary(binary_data)

    assert(decoded_batch:count() == original_batch:count(), "Decoded batch count should match original")
    assert(decoded_batch:start_time() == original_batch:start_time(), "Decoded start time should match")
    assert(decoded_batch:end_time() == original_batch:end_time(), "Decoded end time should match")

    for i = 1, original_batch:count() do
        local original_record = original_batch:get_record(i)
        local decoded_record = decoded_batch:get_record(i)

        assert(original_record:getTimestamp() == decoded_record:getTimestamp(), "Record " .. i .. " timestamp mismatch")
        assert(original_record:get_value("status") == decoded_record:get_value("status"), "Record " .. i .. " status mismatch")
        assert(original_record.nil_flags == decoded_record.nil_flags, "Record " .. i .. " nil_flags mismatch")
    end
end

return test_case