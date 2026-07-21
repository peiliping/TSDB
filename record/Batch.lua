local Columns = require("record.col.Columns")
local Record = require("record.Record")
local BitTools = require("tools.BitTools")
local BinaryTools = require("tools.BinaryTools")

local Batch = {
    columns = nil,
    datas = nil,
    nil_flags = nil,
    filter_nil = nil,
}

Batch.__index = Batch

function Batch.new(columns, filter_nil)
    local self = setmetatable({}, Batch)
    if not columns or type(columns) ~= "table" then
        error("Batch.new: 'columns' must be a table.")
    end
    self.columns = columns
    self.datas = {}
    self.nil_flags = {}
    self.filter_nil = filter_nil or false
    return self
end

function Batch:add(data, nil_flags)
    local ts = data[Columns.TIMESTAMP_COL.id]
    local interval = self.columns:get_interval()
    if ts % interval ~= 0 then
        error("Data Time not match interval.")
    end
    if self.filter_nil then
        if self.columns.nil_record_flags == nil_flags then
            return
        end
    else
        if self:count() > 0 then
            local end_time = self:end_time()
            if ts <= end_time then
                error("Data Time out of order.")
            end
            if ts > end_time + interval then
                for cur_ts = end_time + interval, ts - interval, interval do
                    local r_nil = Record.create_nil_record(self.columns, cur_ts)
                    table.insert(self.datas, r_nil.data)
                    table.insert(self.nil_flags, r_nil.nil_flags)
                end
            end
        end
    end
    if not nil_flags then
        nil_flags = BitTools.calculate_nil_flags(data, self.columns:count())
    end
    table.insert(self.datas, data)
    table.insert(self.nil_flags, nil_flags)
end

function Batch:add_datas(datas)
    for _, data in ipairs(datas) do
        self:add(data)
    end
end

function Batch:add_record(record)
    self:add(record.data, record.nil_flags)
end

function Batch:add_records(records)
    for _, record in ipairs(records) do
        self:add_record(record)
    end
end

function Batch:count()
    return #self.datas
end

function Batch:start_time()
    local c = self:count()
    if c == 0 then
        error("Batch is empty.")
    end
    return self.datas[1][Columns.TIMESTAMP_COL.id]
end

function Batch:end_time()
    local c = self:count()
    if c == 0 then
        error("Batch is empty.")
    end
    return self.datas[c][Columns.TIMESTAMP_COL.id]
end

function Batch:get_record(index)
    local c = self:count()
    if index < 1 or index > c then
        error(string.format("Batch:get_record: Index %d is out of bounds (1 to %d).", index, c))
    end
    return Record.new(self.columns, self.datas[index], self.nil_flags[index])
end

function Batch:to_binary()
    local result = {}
    local cache = {}
    for i, data in ipairs(self.datas) do
        result[i] = BinaryTools.pack_record_data(self.columns, data, self.nil_flags[i], cache)
    end
    return table.concat(result)
end

function Batch:from_binary(binary_string)
    local pos = 1
    local len = #binary_string
    while pos <= len do
        local data_list, nil_flags, next_pos = BinaryTools.unpack_record_data(self.columns, binary_string, pos)
        self:add(data_list, nil_flags)
        pos = next_pos
    end
end

return Batch