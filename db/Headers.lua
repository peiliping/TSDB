local NumberCol = require("record.col.NumberCol")

local H = {
    ES = {
        NumberCol.new("magic", "number", 0, false),
        NumberCol.new("interval", "shortnumber", 0, false),
        NumberCol.new("record_size", "shortnumber", 0, false),
        NumberCol.new("start_time", "number", 0, false),
        NumberCol.new("end_time", "number", 0, false),
        NumberCol.new("crc32", "number", 0, false),
    },
    MAGIC = 2026070100,
    header_length = 0,
    header_format = "",
    crc_format = "",
}

for _, col in pairs(H.ES) do
    H.header_length = H.header_length + col.size
    H.header_format = H.header_format .. col.format
end
H.crc_format = H.ES[4].format .. H.ES[5].format

return H