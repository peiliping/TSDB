local RingBuffer = {
    max_size = nil,
    data = nil,
    head = nil, -- 指向【下一个】要写入的位置
    tail = nil, -- 指向【最早一个】元素的位置
    count = nil,
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
    self.head = 1
    self.tail = 1
    self.count = 0
    return self
end

function RingBuffer:add(ele)
    self.data[self.head] = ele
    if self:is_full() then
        self.tail = (self.tail % self.max_size) + 1
    else
        self.count = self.count + 1
    end
    self.head = (self.head % self.max_size) + 1
end

function RingBuffer:get(id)
    if id < 1 or id > self.count then
        error("Index out of range.")
    end
    -- 2. 计算物理索引
    -- (self.tail - 1)         -- 将 1-based 的 tail 转为 0-based
    -- (id - 1)                -- 将 1-based 的逻辑索引转为 0-based 偏移
    -- (...) % self.max_size   -- 计算 0-based 的物理索引 (处理环绕)
    -- ... + 1                 -- 将结果转回 1-based 供 table 使用
    local index = (self.tail - 1 + id - 1) % self.max_size + 1
    return self.data[index]
end

function RingBuffer:size()
    return self.count
end

function RingBuffer:is_full()
    return self.count == self.max_size
end

function RingBuffer:clear()
    self.head = 1
    self.tail = 1
    self.count = 0
end

return RingBuffer