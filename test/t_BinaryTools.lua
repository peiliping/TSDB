local BinaryTools = require("tools.BinaryTools")
local BitTools = require("tools.BitTools")
local Crc32Tools = require("tools.Crc32Tools")

local test_case = {}

-- Mock Headers module
local MockHeaders = {
    MAGIC = 2026070100,
    header_format = "<I4I4I4I8I8I4", -- Example format: magic, interval, record_size, start_time, end_time, crc
    crc_format = "<I8I8", -- Example format for start_time, end_time for CRC calculation
}

-- Mock Column object for pack_record_data and unpack_record_data
local MockColumn = {}
function MockColumn.new(name, format)
    local self = { name = name, format = format }
    function self:pack_value(value)
        if self.format == "<I4" then -- number
            return value
        elseif self.format == "<I8" then -- long number
            return value
        else
            return value -- default for other types
        end
    end
    function self:unpack_value(packed_value)
        if self.format == "<I4" then
            return packed_value
        elseif self.format == "<I8" then
            return packed_value
        else
            return packed_value
        end
    end
    return self
end

-- Mock Columns object for pack_record_data and unpack_record_data
local MockColumns = {
    cols = {
        MockColumn.new("col1", "<I4"),
        MockColumn.new("col2", "<I8"),
        MockColumn.new("col3", "<I4"),
    },
    format_string = "<I4I4I8I4", -- nil_flags + col1 + col2 + col3
}

-- Override the required modules with mocks for testing
local original_require = require
_G.require = function(module_name)
    if module_name == "db.Headers" then
        return MockHeaders
    end
    return original_require(module_name)
end

-- Re-require BinaryTools to use the mocked Headers
BinaryTools = original_require("tools.BinaryTools")

function test_case.test_pack_header()
    local interval = 60
    local record_size = 12
    local start_time = 1678886400 -- March 15, 2023 00:00:00 UTC
    local end_time = 1678886460   -- March 15, 2023 00:01:00 UTC

    local packed_header = BinaryTools.pack_header(interval, record_size, start_time, end_time)
    assert(packed_header ~= nil, "Packed header should not be nil")

    local expected_crc = Crc32Tools.crc32(string.pack(MockHeaders.crc_format, start_time, end_time))
    local magic, p_interval, p_record_size, p_start_time, p_end_time, p_crc = string.unpack(MockHeaders.header_format, packed_header)

    assert(magic == MockHeaders.MAGIC, "Magic number mismatch")
    assert(p_interval == interval, "Interval mismatch")
    assert(p_record_size == record_size, "Record size mismatch")
    assert(p_start_time == start_time, "Start time mismatch")
    assert(p_end_time == end_time, "End time mismatch")
    assert(p_crc == expected_crc, "CRC mismatch")
end

function test_case.test_unpack_header_valid()
    local interval = 60
    local record_size = 12
    local start_time = 1678886400
    local end_time = 1678886460

    local packed_header = BinaryTools.pack_header(interval, record_size, start_time, end_time)
    local u_start_time, u_end_time = BinaryTools.unpack_header(interval, record_size, packed_header)

    assert(u_start_time == start_time, "Unpacked start time mismatch")
    assert(u_end_time == end_time, "Unpacked end time mismatch")
end

function test_case.test_unpack_header_invalid_magic()
    local interval = 60
    local record_size = 12
    local start_time = 1678886400
    local end_time = 1678886460

    local original_magic = MockHeaders.MAGIC
    MockHeaders.MAGIC = 12345 -- Temporarily change magic
    local packed_header = BinaryTools.pack_header(interval, record_size, start_time, end_time)
    MockHeaders.MAGIC = original_magic -- Restore original magic

    local status, err = pcall(BinaryTools.unpack_header, interval, record_size, packed_header)
    assert(not status, "Should fail with invalid magic number")
    assert(string.find(err, "invalid magic number."), "Error message should indicate invalid magic")
end

function test_case.test_unpack_header_invalid_interval()
    local interval = 60
    local record_size = 12
    local start_time = 1678886400
    local end_time = 1678886460

    local packed_header = BinaryTools.pack_header(interval, record_size, start_time, end_time)

    local status, err = pcall(BinaryTools.unpack_header, 90, record_size, packed_header) -- Pass wrong interval
    assert(not status, "Should fail with invalid interval")
    assert(string.find(err, "invalid interval."), "Error message should indicate invalid interval")
end

function test_case.test_unpack_header_invalid_record_size()
    local interval = 60
    local record_size = 12
    local start_time = 1678886400
    local end_time = 1678886460

    local packed_header = BinaryTools.pack_header(interval, record_size, start_time, end_time)

    local status, err = pcall(BinaryTools.unpack_header, interval, 20, packed_header) -- Pass wrong record_size
    assert(not status, "Should fail with invalid record size")
    assert(string.find(err, "invalid record size."), "Error message should indicate invalid record size")
end

function test_case.test_unpack_header_invalid_crc32()
    local interval = 60
    local record_size = 12
    local start_time = 1678886400
    local end_time = 1678886460

    local packed_header = BinaryTools.pack_header(interval, record_size, start_time, end_time)

    -- Corrupt the CRC in the packed header
    local magic, p_interval, p_record_size, p_start_time, p_end_time, p_crc = string.unpack(MockHeaders.header_format, packed_header)
    local corrupted_crc_header = string.pack(MockHeaders.header_format, magic, p_interval, p_record_size, p_start_time, p_end_time, p_crc + 1)

    local status, err = pcall(BinaryTools.unpack_header, interval, record_size, corrupted_crc_header)
    assert(not status, "Should fail with invalid crc32")
    assert(string.find(err, "invalid crc32"), "Error message should indicate invalid crc32")
end

function test_case.test_pack_record_data_no_nils()
    local data_list = { 100, 2000000000, 300 }
    local nil_flags = 0 -- No nils
    local packed_data = BinaryTools.pack_record_data(MockColumns, data_list, nil_flags)

    assert(packed_data ~= nil, "Packed data should not be nil")
    local u_nil_flags, u_col1, u_col2, u_col3 = string.unpack(MockColumns.format_string, packed_data)
    assert(u_nil_flags == nil_flags, "Nil flags mismatch")
    assert(u_col1 == data_list[1], "Column 1 data mismatch")
    assert(u_col2 == data_list[2], "Column 2 data mismatch")
    assert(u_col3 == data_list[3], "Column 3 data mismatch")
end

function test_case.test_pack_record_data_with_nils()
    local data_list = { nil, 2000000000, nil }
    local nil_flags = BitTools.set_bit(0, 0) -- col1 is nil
    nil_flags = BitTools.set_bit(nil_flags, 2) -- col3 is nil
    -- Expected nil_flags: 0101 (binary) = 5 (decimal)

    local packed_data = BinaryTools.pack_record_data(MockColumns, data_list, nil_flags)
    assert(packed_data ~= nil, "Packed data should not be nil")

    local u_nil_flags, u_col1, u_col2, u_col3 = string.unpack(MockColumns.format_string, packed_data)
    assert(u_nil_flags == nil_flags, "Nil flags mismatch")
    assert(u_col1 == 0, "Nil column 1 should be packed as 0") -- Default value for nil
    assert(u_col2 == data_list[2], "Column 2 data mismatch")
    assert(u_col3 == 0, "Nil column 3 should be packed as 0") -- Default value for nil
end

function test_case.test_unpack_record_data_no_nils()
    local data_list = { 100, 2000000000, 300 }
    local nil_flags = 0
    local packed_data = BinaryTools.pack_record_data(MockColumns, data_list, nil_flags)

    local unpacked_data, u_nil_flags = BinaryTools.unpack_record_data(MockColumns, packed_data, 1)

    assert(u_nil_flags == nil_flags, "Unpacked nil flags mismatch")
    assert(unpacked_data[1] == data_list[1], "Unpacked column 1 data mismatch")
    assert(unpacked_data[2] == data_list[2], "Unpacked column 2 data mismatch")
    assert(unpacked_data[3] == data_list[3], "Unpacked column 3 data mismatch")
end

function test_case.test_unpack_record_data_with_nils()
    local data_list = { nil, 2000000000, nil }
    local nil_flags = BitTools.set_bit(0, 0)
    nil_flags = BitTools.set_bit(nil_flags, 2)
    local packed_data = BinaryTools.pack_record_data(MockColumns, data_list, nil_flags)

    local unpacked_data, u_nil_flags = BinaryTools.unpack_record_data(MockColumns, packed_data, 1)

    assert(u_nil_flags == nil_flags, "Unpacked nil flags mismatch")
    assert(unpacked_data[1] == nil, "Unpacked nil column 1 should be nil")
    assert(unpacked_data[2] == data_list[2], "Unpacked column 2 data mismatch")
    assert(unpacked_data[3] == nil, "Unpacked nil column 3 should be nil")
end

-- Restore original require function after tests
_G.require = original_require

return test_case