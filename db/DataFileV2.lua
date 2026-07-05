local Headers = require("db.Headers")
local BinaryTools = require("tools.BinaryTools")

local DataFile = {
    file_path = nil,
    file_block_size = nil,
    file_size = nil, -- loading
    interval = nil,
    record_size = nil,
    start_time = nil, -- loading
    end_time = nil, -- loading
}

DataFile.__index = DataFile

function DataFile.new(path, block_size, interval, record_size)
    local self = {}
    setmetatable(self, DataFile)
    self.file_path = path
    self.file_block_size = block_size
    self.interval = interval
    self.record_size = record_size
    return self
end

function DataFile:exist()
    local f = io.open(self.file_path, "r")
    if f then
        f:close()
    end
    return f ~= nil
end

function DataFile:create()
    local f = io.open(self.file_path, "r")
    if not f then
        f = io.open(self.file_path, "wb")
        f:write(BinaryTools.pack_header(self.interval, self.record_size, 0, 0))
        f:write(string.rep("\0", self.file_block_size))
        f:flush()
    end
    f:close()
    self.file_size = Headers.header_length + self.file_block_size
    self.start_time = 0
    self.end_time = 0
end

function DataFile.load()
    local f = io.open(self.file_path, "rb")
    if not f then
        error("failed to open file: " .. self.file_path)
    end
    local binary = f:read(Headers.header_length)
    self.file_size = f:seek("end")
    f:close()
    local start_time, end_time = BinaryTools.unpack_header(self.interval, self.record_size, binary)
    self.start_time = start_time
    self.end_time = end_time
end


--
--function DataFile.expand(file, block_size)
--    local current_pos = file:seek("cur")
--    file:seek("end")
--    file:write(string.rep("\0", block_size))
--    file:flush()
--    file:seek("set", current_pos)
--end

return DataFile