local BitTools = require("tools.BitTools")
local Headers = require("db.Headers")

local B = {}

function B.pack_header(interval, record_size, start_time, end_time)
    local crc32Str = string.pack(Headers.crc_format, start_time, end_time)
    local crc32 = CryptoTools.crc32(crc32Str)
    return string.pack(Headers.header_format, Headers.MAGIC, interval, record_size, start_time, end_time, crc32)
end

-- magic, interval, record_size, start_time, end_time, crc32
function B.unpack_header(header_binary_string)
    return string.unpack(Headers.header_format, header_binary_string)
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

function B.unpack_record_data(columns, record_binary_string)
    local unpacked = { string.unpack(columns.format_string, record_binary_string) }
    local nil_flags = unpacked[1]
    local data_list = {}
    for i, col in ipairs(columns.cols) do
        if BitTools.check_bit(nil_flags, i - 1) then
            data_list[i] = nil
        else
            data_list[i] = col:unpack_value(unpacked[i + 1])
        end
    end
    return data_list, nil_flags
end

return B