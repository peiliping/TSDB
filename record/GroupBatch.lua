local Group = {
    data_table = nil,
    interval = nil,
    total_start = nil,
    total_end = nil,
    records_per_batch = nil,
    chunk_duration = nil,
    filter_nil = nil,
    -- cache
    current_start = nil,
    current_batch = nil,
    current_index = nil,
}
Group.__index = Group

function Group.new(data_table, start_time, end_time, records_per_batch, filter_nil)
    local self = setmetatable({}, Group)
    self.data_table = data_table
    self.interval = data_table.interval
    self.total_start = start_time
    self.total_end = end_time
    self.records_per_batch = records_per_batch or 10000
    self.chunk_duration = (self.records_per_batch - 1) * self.interval
    self.filter_nil = filter_nil or false

    self.current_start = start_time
    self.current_index = 0
    return self
end

function Group:next()
    while true do
        if self.current_batch and self.current_index < self.current_batch:count() then
            self.current_index = self.current_index + 1
            return self.current_batch:get_record(self.current_index)
        end
        if self.current_start > self.total_end then
            return nil
        end
        local current_end = math.min(self.current_start + self.chunk_duration, self.total_end)
        self.current_batch = self.data_table:query_records(self.current_start, current_end, self.filter_nil)
        self.current_index = 0
        self.current_start = current_end + self.interval
    end
end

function Group:iterator()
    return function()
        return self:next()
    end
end

return Group