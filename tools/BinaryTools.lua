local BitTools = require("tools.BitTools")

local B = {}

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