local TestTools = require("test.TestTools")

local BitTools = require("tools.BitTools")

local Columns = require("record.col.Columns")
local TimeCol = require("record.col.TimeCol")
local NumberCol = require("record.col.NumberCol")

local Record = require("record.Record")

local test_case = {}

function test_case.test_Record_new()
    local col1 = TimeCol.new(1, "timestamp", 60)
    local col2 = NumberCol.new(2, "value", "number", 2, false)
    local real_cols = Columns.new({ col1, col2 })

    local data = { 12345 * 60, 10.5 }
    local record = Record.new(real_cols, data)
    assert(record ~= nil, "Record should be created")
    assert(record.columns == real_cols, "Columns should match")
    assert(record.data[1] == data[1], "Data timestamp should match")
    assert(math.abs(record.data[2] - data[2]) < 0.001, "Data value should match within tolerance")
    assert(record.nil_flags == 0, "nil_flags should be 0 for no nils")

    local data_with_nil = { 12346 * 60, nil }
    local explicit_nil_flags = BitTools.set_bit(0, 1)
    local record_explicit_nil = Record.new(real_cols, data_with_nil, explicit_nil_flags)
    assert(record_explicit_nil.nil_flags == explicit_nil_flags, "Explicit nil_flags should be used")

    local data_auto_nil = { 12347 * 60, nil }
    local record_auto_nil = Record.new(real_cols, data_auto_nil)
    assert(record_auto_nil.nil_flags == explicit_nil_flags, "nil_flags should be auto-calculated correctly")

    TestTools.assert_error_msg_contains("'columns' must be an a table.", function()
        Record.new(nil, data)
    end)

    TestTools.assert_error_msg_contains("'data_list' must be a table.", function()
        Record.new(real_cols, nil)
    end)

    local data_too_many = { 123 * 60, 45.6, 789 }
    TestTools.assert_error_msg_contains("Column definition count does not match data value count.", function()
        Record.new(real_cols, data_too_many)
    end)
end

function test_case.test_create_nil_record()
    local col1 = TimeCol.new(1, "timestamp", 60)
    local col2 = NumberCol.new(2, "value", "number", 2, false)
    local real_cols = Columns.new({ col1, col2 })

    local timestamp = os.time()
    local aligned_timestamp = math.floor(timestamp / col1.interval) * col1.interval
    local nil_record = Record.create_nil_record(real_cols, aligned_timestamp)

    assert(nil_record ~= nil, "Nil record should be created")
    assert(nil_record.columns == real_cols, "Columns should match")
    assert(nil_record.data[1] == aligned_timestamp, "Timestamp should be set")
    assert(nil_record.data[2] == nil, "Other data should be nil")
    assert(nil_record.nil_flags == real_cols.nil_record_flags, "nil_flags should match nil_record_flags")
    assert(nil_record:is_nil_record(), "Should be identified as a nil record")
end

function test_case.test_binary_serialization()
    local col1 = TimeCol.new(1, "timestamp", 60)
    local col2 = NumberCol.new(2, "value", "number", 2, false)
    local col3 = NumberCol.new(3, "status", "number", 0, false)
    local real_cols = Columns.new({ col1, col2, col3 })

    local data = { math.floor(os.time() / 60) * 60, 123.45, 1 }
    local record = Record.new(real_cols, data)

    local binary_data = record:to_binary()
    assert(binary_data ~= nil and #binary_data > 0, "Binary data should not be empty")

    local decoded_record = Record.from_binary(real_cols, binary_data)
    assert(decoded_record ~= nil, "Decoded record should not be nil")
    assert(decoded_record:get_timestamp() == record:get_timestamp(), "Timestamp should match")
    assert(decoded_record:get_value("status") == record:get_value("status"), "Status should match")
    assert(math.abs(decoded_record:get_value("value") - record:get_value("value")) < 0.001, "Value should match within tolerance")
    assert(decoded_record.nil_flags == record.nil_flags, "Nil flags should match")

    local data_with_nil = { math.floor((os.time() + 1) / 60) * 60, nil, 0 }
    local record_with_nil = Record.new(real_cols, data_with_nil)
    local binary_data_with_nil = record_with_nil:to_binary()
    local decoded_record_with_nil = Record.from_binary(real_cols, binary_data_with_nil)

    assert(decoded_record_with_nil:get_timestamp() == record_with_nil:get_timestamp(), "Timestamp (nil) should match")
    assert(decoded_record_with_nil:get_value("value") == nil, "Nil value should be nil")
    assert(decoded_record_with_nil:get_value("status") == record_with_nil:get_value("status"), "Status (nil) should match")
    assert(decoded_record_with_nil.nil_flags == record_with_nil.nil_flags, "Nil flags (nil) should match")
end

function test_case.test_data_access()
    local col1 = TimeCol.new(1, "timestamp", 60)
    local col2 = NumberCol.new(2, "temperature", "number", 1, false)
    local col3 = NumberCol.new(3, "humidity", "number", 0, false)
    local real_cols = Columns.new({ col1, col2, col3 })

    local timestamp = math.floor(os.time() / 60) * 60
    local record = Record.new(real_cols, { timestamp, 25.5, 60 })

    assert(record:get_timestamp() == timestamp, "get_timestamp should return correct value")

    assert(math.abs(record:get_value("temperature") - 25.5) < 0.001, "get_value should return correct value by name")
    assert(record:get_value("humidity") == 60, "get_value should return correct value by name")
    TestTools.assert_error_msg_contains("Column not found with name: non_existent", function()
        record:get_value("non_existent")
    end)

    assert(record:get_value_by_index(1) == timestamp, "get_value_by_index should return correct value")
    assert(math.abs(record:get_value_by_index(2) - 25.5) < 0.001, "get_value_by_index should return correct value")
    TestTools.assert_error_msg_contains("Index 0 is out of bounds", function()
        record:get_value_by_index(0)
    end)
    TestTools.assert_error_msg_contains("Index 4 is out of bounds", function()
        record:get_value_by_index(4)
    end)

    assert(not record:is_column_nil("temperature"), "temperature should not be nil")
    assert(not record:is_column_nil_by_index(2), "temperature (index) should not be nil")

    record:set_value("temperature", 26.0)
    assert(math.abs(record:get_value("temperature") - 26.0) < 0.001, "set_value should update value")
    assert(not record:is_column_nil("temperature"), "temperature should still not be nil after update")

    record:set_value("temperature", nil)
    assert(record:get_value("temperature") == nil, "set_value should set to nil")
    assert(record:is_column_nil("temperature"), "temperature should be nil after setting nil")

    record:set_value_by_index(3, 65)
    assert(record:get_value_by_index(3) == 65, "set_value_by_index should update value")
    assert(not record:is_column_nil_by_index(3), "humidity should still not be nil after update")

    record:set_value_by_index(3, nil)
    assert(record:get_value_by_index(3) == nil, "set_value_by_index should set to nil")
    assert(record:is_column_nil_by_index(3), "humidity should be nil after setting nil")

    TestTools.assert_error_msg_contains("Index 0 is out of bounds", function()
        record:set_value_by_index(0, 10)
    end)
    TestTools.assert_error_msg_contains("Index 4 is out of bounds", function()
        record:set_value_by_index(4, 10)
    end)
end

return test_case
