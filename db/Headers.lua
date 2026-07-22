local NumberCol = require("record.col.NumberCol")

local H = {
    ES = {
        NumberCol.new(1, "magic", "number", 0, false),
        NumberCol.new(2, "interval", "shortnumber", 0, false),
        NumberCol.new(3, "record_size", "shortnumber", 0, false),
        NumberCol.new(4, "start_time", "number", 0, false),
        NumberCol.new(5, "end_time", "number", 0, false),
        NumberCol.new(6, "crc32", "number", 0, false),
    },
    MAGIC = 2026070100,
    header_length = 0,
    header_format = "",
    crc_format = "",
}

for _, col in ipairs(H.ES) do
    H.ES[col.name] = col
    H.header_length = H.header_length + col.size
    H.header_format = H.header_format .. col.format
end

H.crc_format = H.ES["start_time"].format .. H.ES["end_time"].format

return H