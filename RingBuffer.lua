local M = {}

local RingBuffer = {}
RingBuffer.__index = RingBuffer

function M.new(maxSize)
    assert(maxSize and maxSize > 0, "maxSize must be positive.")
    local self = {
        maxSize = maxSize,
        data = {},
        head = 1,   -- 指向【下一个】要写入的位置
        tail = 1,   -- 指向【最早一个】元素的位置
        count = 0,
    }
    return setmetatable(self, RingBuffer)
end

function RingBuffer:add(element)
    self.data[self.head] = element
    self.head = (self.head % self.maxSize) + 1
    if self.count == self.maxSize then
        self.tail = (self.tail % self.maxSize) + 1
    else
        self.count = self.count + 1
    end
end

function RingBuffer:get(idx)
    if idx < 1 or idx > self.count then
        return nil -- 索引越界
    end
    -- 2. 计算物理索引
    -- (self.tail - 1)          -- 将 1-based 的 tail 转为 0-based
    -- (idx - 1)               -- 将 1-based 的逻辑索引转为 0-based 偏移
    -- (...) % self.max_size   -- 计算 0-based 的物理索引 (处理环绕)
    -- ... + 1                 -- 将结果转回 1-based 供 table 使用
    local physicalIndex = ((self.tail - 1) + (idx - 1)) % self.maxSize + 1
    return self.data[physicalIndex]
end

function RingBuffer:getAll()
    local result = {}
    local currentIndex = self.tail
    for i = 1, self.count do
        result[i] = self.data[currentIndex]
        currentIndex = (currentIndex % self.maxSize) + 1
    end
    return result
end

function RingBuffer:size()
    return self.count
end

function RingBuffer:isFull()
    return self.count == self.maxSize
end

function RingBuffer:clear()
    self.head = 1
    self.tail = 1
    self.count = 0
end

return M