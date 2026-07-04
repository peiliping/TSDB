local Headers = require("db.Headers")
local BinaryTools = require("tools.BinaryTools")

local DataFile = {
    file_block_size = nil,
    interval = nil,
    record_size = nil,
    start_time = nil,
    end_time = nil,
    crc32 = nil,
}

DataFile.__index = DataFile

function DataFile.create(path, block_size, interval, record_size)
    local f = io.open(path, "r")
    if f then
        f:close()
        error("file " .. path .. " is exist")
    end
    f = io.open(path, "wb")
    f:write(BinaryTools.pack_header(interval, record_size, 0, 0))
    f:write(string.rep("\0", block_size))
    f:flush()
    f:close()
end

function DataFile.load(path, block_size, interval, record_size)
    local self = {}
    setmetatable(self, DataFile)
    self.file_block_size = block_size
    self.interval = interval
    self.record_size = record_size

    local f = io.open(path, "rb")
    if not f then
        error("failed to open file: " .. path)
    end
    local binary = f:read(Headers.header_length)
    local magic, interval, record_size, start_time, end_time, crc32 = BinaryTools.unpack_header(binary)
    assert(Headers.MAGIC == magic, "invalid magic number.")
    assert(self.interval == interval, "invalid interval.")
    assert(self.record_size == record_size, "invalid record size.")
    assert(Headers.crc32(start_time, end_time) == crc32, "invalid crc32")
    f:close()
    self.start_time = start_time
    self.end_time = end_time
    self.crc32 = crc32
    return self
end

return DataFile