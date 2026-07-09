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
    local self = setmetatable({}, DataFile)
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

local function get_file(path, mode)
    local f, err = io.open(path, mode)
    if not f then
        error("failed to open file: " .. path .. " err: " .. tostring(err))
    end
    return f
end

function DataFile:create()
    local f = io.open(self.file_path, "r")
    if not f then
        f = get_file(self.file_path, "wb")
        f:write(BinaryTools.pack_header(self.interval, self.record_size, 0, 0))
        f:write(string.rep("\0", self.file_block_size))
        f:flush()
    end
    f:close()
    self.file_size = Headers.header_length + self.file_block_size
    self.start_time = 0
    self.end_time = 0
end

function DataFile:load()
    local f = get_file(self.file_path, "rb")
    local binary = f:read(Headers.header_length)
    self.file_size = f:seek("end")
    f:close()
    local start_time, end_time = BinaryTools.unpack_header(binary, self.interval, self.record_size)
    self.start_time = start_time
    self.end_time = end_time
end

function DataFile:write(batch)
    if batch:count() == 0 then
        return 0
    end
    local b_start_time = batch:start_time()
    local b_end_time = batch:end_time()
    local batch_binary = batch:toBinary()
    local batch_len = #batch_binary
    if batch:count() ~= math.floor((b_end_time - b_start_time) / self.interval + 1) then
        error("Batch Data Gap detected (discontinuity not allowed).")
    end
    local update_header = false
    local cur_pos
    if self.start_time == 0 then
        cur_pos = Headers.header_length
        self.start_time = b_start_time
        update_header = true
    else
        if b_start_time < self.start_time then
            error(string.format("Out of range: batch start_time %d < file start_time %d", b_start_time, self.start_time))
        end
        if b_start_time > (self.end_time + self.interval) then
            error("TimeSeries Data Gap detected (discontinuity not allowed).")
        end
        local offset_count = math.floor((b_start_time - self.start_time) / self.interval)
        cur_pos = Headers.header_length + offset_count * self.record_size
    end
    local f = get_file(self.file_path, "r+b")
    if cur_pos + batch_len > self.file_size then
        local exp_block_count = math.ceil((cur_pos + batch_len - self.file_size) / self.file_block_size)
        for i = 1, exp_block_count do
            f:seek("end")
            f:write(string.rep("\0", self.file_block_size))
            self.file_size = self.file_size + self.file_block_size
        end
        f:flush()
    end
    f:seek("set", cur_pos)
    f:write(batch_binary)
    f:flush()
    if b_end_time > self.end_time then
        self.end_time = b_end_time
        update_header = true
    end
    if update_header then
        f:seek("set", 0)
        f:write(BinaryTools.pack_header(self.interval, self.record_size, self.start_time, self.end_time))
        f:flush()
    end
    f:close()
    return batch:count()
end

function DataFile:read(batch, start_time, end_time)
    start_time = math.max(start_time, self.start_time)
    end_time = math.min(end_time, self.end_time)
    if end_time < start_time then
        return
    end
    local offset_count = math.floor((start_time - self.start_time) / self.interval)
    local cur_pos = Headers.header_length + offset_count * self.record_size
    local read_len = ((end_time - start_time) / self.interval + 1) * self.record_size

    local f = get_file(self.file_path, "rb")
    f:seek("set", cur_pos)
    local binary_string = f:read(read_len)
    f:close()
    if binary_string and #binary_string > 0 then
        batch:fromBinary(binary_string)
    end
end

function DataFile:count()
    if self.end_time == 0 then
        return 0
    end
    return math.floor((self.end_time - self.start_time) / self.interval + 1)
end

return DataFile