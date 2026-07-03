local C = {}

local crc32_table = {}
for i = 0, 255 do
    local crc = i
    for j = 1, 8 do
        if (crc & 1) == 1 then
            crc = (crc >> 1) ~ 0xEDB88320
        else
            crc = crc >> 1
        end
    end
    crc32_table[i] = crc
end

function C.crc32(data)
    local crc = 0xFFFFFFFF
    for i = 1, #data do
        local byte_val = string.byte(data, i)
        local index = (crc & 0xFF) ~ byte_val
        crc = (crc >> 8) ~ crc32_table[index]
    end
    return crc ~ 0xFFFFFFFF
end

return C