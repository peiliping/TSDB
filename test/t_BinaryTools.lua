local BinaryTools = require("tools.BinaryTools")
local Headers = require("db.Headers")
local BitTools = require("tools.BitTools")
local TestTools = require("test.TestTools")

local test_case = {}

-- Test for pack_header and unpack_header
function test_case.test_HeaderSerialization()
    local interval = 60
    local record_size = 16
    local start_time = os.time() - 3600
    local end_time = os.time()

    local packed_header = BinaryTools.pack_header(interval, record_size, start_time, end_time)
    assert(packed_header ~= nil, "packed_header should not be nil")
    assert(#packed_header == Headers.header_length, "packed_header length should match Headers.header_length")

    local unpacked_start_time, unpacked_end_time = BinaryTools.unpack_header(interval, record_size, packed_header)
    assert(start_time == unpacked_start_time, "start_time should match unpacked_start_time")
    assert(end_time == unpacked_end_time, "end_time should match unpacked_end_time")

    -- Test with invalid magic number
    local invalid_magic_header = string.pack(Headers.header_format, Headers.MAGIC + 1, interval, record_size, start_time, end_time, 0)
    TestTools.assertErrorMsgContains("invalid magic number.", function() -- Changed
        BinaryTools.unpack_header(interval, record_size, invalid_magic_header)
    end)

    -- Test with invalid interval
    local invalid_interval_header = string.pack(Headers.header_format, Headers.MAGIC, interval + 1, record_size, start_time, end_time, 0)
    TestTools.assertErrorMsgContains("invalid interval.", function() -- Changed
        BinaryTools.unpack_header(interval, record_size, invalid_interval_header)
    end)

    -- Test with invalid record size
    local invalid_record_size_header = string.pack(Headers.header_format, Headers.MAGIC, interval, record_size + 1, start_time, end_time, 0)
    TestTools.assertErrorMsgContains("invalid record size.", function() -- Changed
        BinaryTools.unpack_header(interval, record_size, invalid_record_size_header)
    end)

    -- Test with invalid crc32 (by modifying end_time in packed header)
    local original_crc = select(6, string.unpack(Headers.header_format, packed_header)) -- Get original crc
    local modified_header = string.pack(Headers.header_format, Headers.MAGIC, interval, record_size, start_time, end_time + 1, original_crc)
    TestTools.assertErrorMsgContains("invalid crc32", function() -- Changed
        BinaryTools.unpack_header(interval, record_size, modified_header)
    end)
end

-- Mock Column object for record serialization tests
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
    if self.format == "i4" then
        -- integer
        return value
    elseif self.format == "f4" then
        -- float
        return value
    else
        return value -- default for other types
    end
end

function MockColumn:unpack_value(value)
    return value
end

-- Test for pack_record_data and unpack_record_data
function test_case.test_RecordSerialization()
    local col1 = MockColumn:new("col1", "I4", 4)
    local col2 = MockColumn:new("col2", "f", 4)
    local col3 = MockColumn:new("col3", "I4", 4)

    local mock_columns = {
        cols = { col1, col2, col3 },
        format_string = "<I4" .. col1.format .. col2.format .. col3.format,
    }

    local data_list = { 123, 45.67, 789 }
    local nil_flags = 0 -- No nils

    local packed_record = BinaryTools.pack_record_data(mock_columns, data_list, nil_flags)
    assert(packed_record ~= nil, "packed_record should not be nil")

    local unpacked_data, unpacked_nil_flags = BinaryTools.unpack_record_data(mock_columns, packed_record, 1)
    assert(nil_flags == unpacked_nil_flags, "nil_flags should match unpacked_nil_flags")
    assert(data_list[1] == unpacked_data[1], "data_list[1] should match unpacked_data[1]")
    assert(data_list[3] == unpacked_data[3], "data_list[3] should match unpacked_data[3]")

    -- Test with nil values
    local data_list_with_nil = { 111, nil, 333 }
    local nil_flags_with_nil = BitTools.set_bit(0, 1) -- Set bit 1 for col2 (index 1)

    local packed_record_with_nil = BinaryTools.pack_record_data(mock_columns, data_list_with_nil, nil_flags_with_nil)
    assert(packed_record_with_nil ~= nil, "packed_record_with_nil should not be nil")

    local unpacked_data_with_nil, unpacked_nil_flags_with_nil = BinaryTools.unpack_record_data(mock_columns, packed_record_with_nil, 1)
    assert(nil_flags_with_nil == unpacked_nil_flags_with_nil, "nil_flags_with_nil should match unpacked_nil_flags_with_nil")
    assert(data_list_with_nil[1] == unpacked_data_with_nil[1], "data_list_with_nil[1] should match unpacked_data_with_nil[1]")
    assert(unpacked_data_with_nil[2] == nil, "unpacked_data_with_nil[2] should be nil")
    assert(data_list_with_nil[3] == unpacked_data_with_nil[3], "data_list_with_nil[3] should match unpacked_data_with_nil[3]")

    -- Test with all nil values
    local data_list_all_nil = { nil, nil, nil }
    local nil_flags_all_nil = BitTools.set_bit(BitTools.set_bit(BitTools.set_bit(0, 0), 1), 2)

    local packed_record_all_nil = BinaryTools.pack_record_data(mock_columns, data_list_all_nil, nil_flags_all_nil)
    assert(packed_record_all_nil ~= nil, "packed_record_all_nil should not be nil")

    local unpacked_data_all_nil, unpacked_nil_flags_all_nil = BinaryTools.unpack_record_data(mock_columns, packed_record_all_nil, 1)
    assert(nil_flags_all_nil == unpacked_nil_flags_all_nil, "nil_flags_all_nil should match unpacked_nil_flags_all_nil")
    assert(unpacked_data_all_nil[1] == nil, "unpacked_data_all_nil[1] should be nil")
    assert(unpacked_data_all_nil[2] == nil, "unpacked_data_all_nil[2] should be nil")
    assert(unpacked_data_all_nil[3] == nil, "unpacked_data_all_nil[3] should be nil")
end

return test_case