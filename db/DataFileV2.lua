local Headers = require("db.Headers")

local DF = {
    file_path = nil,
    file_expansion_size = 4 * 1024 * 1024,
    interval = nil,
    record_size = nil,
    start_time = nil,
    end_time = nil,
    crc32 = nil,
}

DF.__index = DF

function DF.new(path, interval, record_size)
    local self = {}
    setmetatable(self, DF)
    self.file_path = path
    self.interval = interval
    self.record_size = record_size
    return self
end

return DF