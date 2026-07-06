local BitTools = require("tools.BitTools")
local Headers = require("db.Headers")
local Crc32Tools = require("tools.Crc32Tools")

local B = {}

local function crc32(start_time, end_time)
    return Crc32Tools.crc32(string.pack(Headers.crc_format, start_time, end_time))
end

function B.pack_header(interval, record_size, start_time, end_time)
    local crc = crc32(start_time, end_time)
    return string.pack(Headers.header_format, Headers.MAGIC, interval, record_size, start_time, end_time, crc)
end

function B.unpack_header(_interval, _record_size, header_bin)
    local magic, interval, record_size, start_time, end_time, crc = string.unpack(Headers.header_format, header_bin)
    assert(Headers.MAGIC == magic, "invalid magic number.")
    assert(_interval == interval, "invalid interval.")
    assert(_record_size == record_size, "invalid record size.")
    assert(crc32(start_time, end_time) == crc, "invalid crc32")
    return start_time, end_time
end

function B.pack_record_data(columns, data_list, nil_flags)
    local packed_data = {}
    for i, col in ipairs(columns.cols) do
        if BitTools.check_bit(nil_flags, i - 1) then
            packed_data[i] = 0
        else
            packed_data[i] = col:pack_value(data_list[i])
        end
    end
    return string.pack(columns.format_string, nil_flags, table.unpack(packed_data))
end

function B.unpack_record_data(columns, record_binary, offset)
    local unpacked = { string.unpack(columns.format_string, record_binary, offset) }
    local nil_flags = unpacked[1]
    local data_list = {}
    for i, col in ipairs(columns.cols) do
        if BitTools.check_bit(nil_flags, i - 1) then
            data_list[i] = nil
        else
            data_list[i] = col:unpack_value(unpacked[i + 1])
        end
    end
    return data_list, nil_flags, unpacked[#unpacked]
end

return B