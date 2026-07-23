local Headers = require("db.Headers")
local BitTools = require("tools.BitTools")
local Crc32Tools = require("tools.Crc32Tools")

local B = {}

local function crc32(start_time, end_time)
    return Crc32Tools.crc32(string.pack(Headers.crc_format, start_time, end_time))
end

function B.pack_header(interval, record_size, start_time, end_time)
    local crc = crc32(start_time, end_time)
    return string.pack(Headers.header_format, Headers.MAGIC, interval, record_size, start_time, end_time, crc)
end

function B.unpack_header(header_bin, _interval, _record_size)
    local magic, interval, record_size, start_time, end_time, crc = string.unpack(Headers.header_format, header_bin)
    if Headers.MAGIC ~= magic then
        error("invalid magic number.")
    end
    if _interval ~= interval then
        error("invalid interval.")
    end
    if _record_size ~= record_size then
        error("invalid record size.")
    end
    if crc32(start_time, end_time) ~= crc then
        error("invalid crc32")
    end
    return start_time, end_time
end

function B.pack_record_data(columns, data_list, nil_flags, _cache)
    if not _cache then
        _cache = {}
    end
    for i, col in ipairs(columns.cols) do
        if BitTools.check_bit(nil_flags, i - 1) then
            _cache[i] = 0
        else
            _cache[i] = col:pack_value(data_list[i])
        end
    end
    return string.pack(columns.format_string, nil_flags, table.unpack(_cache))
end

function B.unpack_record_data(columns, record_binary, offset)
    local unpacked = { string.unpack(columns.format_string, record_binary, offset) }
    local unpacked_count = #unpacked
    local nil_flags = unpacked[1]
    local noffset = unpacked[unpacked_count]
    for i, col in ipairs(columns.cols) do
        if BitTools.check_bit(nil_flags, i - 1) then
            unpacked[i] = nil
        else
            unpacked[i] = col:unpack_value(unpacked[i + 1])
        end
    end
    unpacked[unpacked_count - 1] = nil
    unpacked[unpacked_count] = nil
    return unpacked, nil_flags, noffset
end

return B
