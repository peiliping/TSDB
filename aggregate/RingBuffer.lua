local RingBuffer = {
    max_size = nil,
    data = nil,
    index = nil,
}

RingBuffer.__index = RingBuffer

function RingBuffer.new(max_size)
    local self = {}
    setmetatable(self, RingBuffer)
    if not max_size or max_size <= 0 then
        error("max_size must be positive.")
    end
    self.max_size = max_size
    self.data = {}
    self.index = 0
    return self
end

function RingBuffer:add(ele)
    self.data[self.index % self.max_size + 1] = ele
    self.index = self.index + 1
    if self.index == (self.max_size * 2) then
        self.index = self.index - self.max_size
    end
end

function RingBuffer:get(id)
    local current_size = self:size()
    if id < 1 or id > current_size then
        error("Index out of range.")
    end
    local start = 0
    if self.index <= self.max_size then
        start = 1
    else
        start = self.index - self.max_size + 1
    end
    local pos = start + id - 1
    if pos > self.max_size then
        pos = pos - self.max_size
    end
    return self.data[pos]
end

function RingBuffer:size()
    return math.min(self.index, self.max_size)
end

function RingBuffer:is_full()
    return self.index >= self.max_size
end

function RingBuffer:clear()
    self.index = 0
    for i = 1, self.max_size do
        self.data[i] = nil
    end
end

return RingBuffer