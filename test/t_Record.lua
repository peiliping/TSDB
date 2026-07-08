local Record = require("record.Record")
local BitTools = require("tools.BitTools")
local BinaryTools = require("tools.BinaryTools")

local test_case = {}

-- Helper function for asserting errors
local function assertErrorMsgContains(expected_msg, func)
    local success, err = pcall(func)
    assert(not success, "Expected an error, but no error occurred.")
    assert(string.find(err, expected_msg), "Error message '" .. err .. "' does not contain '" .. expected_msg .. "'")
end

-- Mock Column object (reused from t_BinaryTools.lua)
local MockColumn = {}
function MockColumn:new(name, format, size)
    local o = {
        name = name,
        format = format,
        size = size,
    }
    setmetatable(o, self)
    self.__index = self
    return o
end

function MockColumn:pack_value(value)
    if self.format == "I4" then -- integer
        return value or 0 -- Pack nil as 0 for integer types
    elseif self.format == "f" then -- float
        return value or 0.0 -- Pack nil as 0.0 for float types
    else
        return value -- default for other types
    end
end

function MockColumn:unpack_value(value)
    return value
end

-- Mock Columns object for Record tests
local MockColumns = {}
function MockColumns:new(cols_definition)
    local o = {
        cols = cols_definition,
        name_to_index = {},
        nil_record_flags = 0,
        format_string = "I", -- Assuming nil_flags is an unsigned int
    }
    setmetatable(o, self)
    self.__index = self

    for i, col in ipairs(cols_definition) do
        o.name_to_index[col.name] = i
        o.format_string = o.format_string .. col.format
        -- Calculate nil_record_flags: all bits set for nil record
        o.nil_record_flags = BitTools.set_bit(o.nil_record_flags, i - 1)
    end
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

-- Test for Record.new
function test_case.test_Record_new()
    local col1 = MockColumn:new("timestamp", "I4", 4)
    local col2 = MockColumn:new("value", "f", 4)
    local mock_cols = MockColumns:new({ col1, col2 })

    -- Test normal creation
    local data = { 12345, 10.5 }
    local record = Record.new(mock_cols, data)
    assert(record ~= nil, "Record should be created")
    assert(record.columns == mock_cols, "Columns should match")
    assert(record.data == data, "Data should match")
    assert(record.nil_flags == 0, "nil_flags should be 0 for no nils")

    -- Test creation with explicit nil_flags
    local data_with_nil = { 12346, nil }
    local explicit_nil_flags = BitTools.set_bit(0, 1) -- value is nil
    local record_explicit_nil = Record.new(mock_cols, data_with_nil, explicit_nil_flags)
    assert(record_explicit_nil.nil_flags == explicit_nil_flags, "Explicit nil_flags should be used")

    -- Test creation with data_list containing nils, nil_flags auto-calculated
    local data_auto_nil = { 12347, nil }
    local record_auto_nil = Record.new(mock_cols, data_auto_nil)
    assert(record_auto_nil.nil_flags == explicit_nil_flags, "nil_flags should be auto-calculated correctly")

    -- Test invalid columns
    assertErrorMsgContains("'columns' must be an a table.", function()
        Record.new(nil, data)
    end)

    -- Test invalid data_list
    assertErrorMsgContains("'data_list' must be a table.", function()
        Record.new(mock_cols, nil)
    end)

    -- Test column count mismatch
    local data_too_many = { 123, 45.6, 789 }
    assertErrorMsgContains("Column definition count does not match data value count.", function()
        Record.new(mock_cols, data_too_many)
    end)
end

-- Test for Record.create_nil_record
function test_case.test_create_nil_record()
    local col1 = MockColumn:new("timestamp", "I4", 4)
    local col2 = MockColumn:new("value", "f", 4)
    local mock_cols = MockColumns:new({ col1, col2 })

    local timestamp = os.time()
    local nil_record = Record.create_nil_record(mock_cols, timestamp)

    assert(nil_record ~= nil, "Nil record should be created")
    assert(nil_record.columns == mock_cols, "Columns should match")
    assert(nil_record.data[1] == timestamp, "Timestamp should be set")
    assert(nil_record.data[2] == nil, "Other data should be nil")
    assert(nil_record.nil_flags == mock_cols.nil_record_flags, "nil_flags should match nil_record_flags")
    assert(nil_record:is_nil_record(), "Should be identified as a nil record")
end

-- Test for Record:toBinary and Record.fromBinary
function test_case.test_binary_serialization()
    local col1 = MockColumn:new("timestamp", "I4", 4)
    local col2 = MockColumn:new("value", "f", 4)
    local col3 = MockColumn:new("status", "I4", 4)
    local mock_cols = MockColumns:new({ col1, col2, col3 })

    local data = { os.time(), 123.45, 1 }
    local record = Record.new(mock_cols, data)

    local binary_data = record:toBinary()
    assert(binary_data ~= nil and #binary_data > 0, "Binary data should not be empty")

    local decoded_record = Record.fromBinary(mock_cols, binary_data)
    assert(decoded_record ~= nil, "Decoded record should not be nil")
    assert(decoded_record:getTimestamp() == record:getTimestamp(), "Timestamp should match")
    assert(decoded_record:get_value("status") == record:get_value("status"), "Status should match")
    assert(decoded_record.nil_flags == record.nil_flags, "Nil flags should match")

    -- Test with nil values
    local data_with_nil = { os.time() + 1, nil, 0 }
    local record_with_nil = Record.new(mock_cols, data_with_nil)
    local binary_data_with_nil = record_with_nil:toBinary()
    local decoded_record_with_nil = Record.fromBinary(mock_cols, binary_data_with_nil)

    assert(decoded_record_with_nil:getTimestamp() == record_with_nil:getTimestamp(), "Timestamp (nil) should match")
    assert(decoded_record_with_nil:get_value("value") == nil, "Nil value should be nil")
    assert(decoded_record_with_nil:get_value("status") == record_with_nil:get_value("status"), "Status (nil) should match")
    assert(decoded_record_with_nil.nil_flags == record_with_nil.nil_flags, "Nil flags (nil) should match")
end

-- Test for data access methods
function test_case.test_data_access()
    local col1 = MockColumn:new("timestamp", "I4", 4)
    local col2 = MockColumn:new("temperature", "f", 4)
    local col3 = MockColumn:new("humidity", "I4", 4)
    local mock_cols = MockColumns:new({ col1, col2, col3 })

    local timestamp = os.time()
    local record = Record.new(mock_cols, { timestamp, 25.5, 60 })

    -- getTimestamp
    assert(record:getTimestamp() == timestamp, "getTimestamp should return correct value")

    -- get_value
    assert(record:get_value("temperature") == 25.5, "get_value should return correct value by name")
    assert(record:get_value("humidity") == 60, "get_value should return correct value by name")
    assertErrorMsgContains("Column not found: non_existent", function()
        record:get_value("non_existent")
    end)

    -- get_value_by_index
    assert(record:get_value_by_index(1) == timestamp, "get_value_by_index should return correct value")
    assert(record:get_value_by_index(2) == 25.5, "get_value_by_index should return correct value")
    assertErrorMsgContains("Index 0 is out of bounds", function()
        record:get_value_by_index(0)
    end)
    assertErrorMsgContains("Index 4 is out of bounds", function()
        record:get_value_by_index(4)
    end)

    -- is_column_nil / is_column_nil_by_index
    assert(not record:is_column_nil("temperature"), "temperature should not be nil")
    assert(not record:is_column_nil_by_index(2), "temperature (index) should not be nil")

    -- set_value
    record:set_value("temperature", 26.0)
    assert(record:get_value("temperature") == 26.0, "set_value should update value")
    assert(not record:is_column_nil("temperature"), "temperature should still not be nil after update")

    record:set_value("temperature", nil)
    assert(record:get_value("temperature") == nil, "set_value should set to nil")
    assert(record:is_column_nil("temperature"), "temperature should be nil after setting nil")

    -- set_value_by_index
    record:set_value_by_index(3, 65)
    assert(record:get_value_by_index(3) == 65, "set_value_by_index should update value")
    assert(not record:is_column_nil_by_index(3), "humidity should still not be nil after update")

    record:set_value_by_index(3, nil)
    assert(record:get_value_by_index(3) == nil, "set_value_by_index should set to nil")
    assert(record:is_column_nil_by_index(3), "humidity should be nil after setting nil")

    assertErrorMsgContains("Index 0 is out of bounds", function()
        record:set_value_by_index(0, 10)
    end)
    assertErrorMsgContains("Index 4 is out of bounds", function()
        record:set_value_by_index(4, 10)
    end)
end

return test_case