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
    local self = {}
    setmetatable(self, Batch)
    if not columns or type(columns) ~= "table" then
        error("Batch.new: 'columns' must be a table (Columns object).")
    end
    self.columns = columns
    self.datas = {}
    self.nil_flags = {}
    self.filter_nil = filter_nil or false
    return self
end

function Batch:add(data, nil_flags)
    if not nil_flags then
        nil_flags = BitTools.calculate_nil_flags(data)
    end
    if self.filter_nil then
        if self.columns.nil_record_flags == nil_flags then
            return
        end
    else
        if self:count() > 0 then
            local ts = data[1]
            local interval = self.columns:get_interval()
            assert(ts > self:end_time(), "Data Time out of order.")
            if ts > self:end_time() + interval then
                local current_ts = self:end_time() + interval
                while current_ts < ts do
                    local r_nil = Record.create_nil_record(self.columns, current_ts)
                    table.insert(self.datas, r_nil.data)
                    table.insert(self.nil_flags, r_nil.nil_flags)
                    current_ts = current_ts + interval
                end
            end
        end
    end
    table.insert(self.datas, data)
    table.insert(self.nil_flags, nil_flags)
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
    assert(self:count() > 0, "Batch is empty.")
    return self.datas[1][1]
end

function Batch:end_time()
    assert(self:count() > 0, "Batch is empty.")
    return self.datas[#self.datas][1]
end

function Batch:get_record(index)
    if index < 1 or index > #self.datas then
        error(string.format("Batch:get_record: Index %d is out of bounds (1 to %d).", index, #self.datas))
    end
    return Record.new(self.columns, self.datas[index], self.nil_flags[index])
end

function Batch:toBinary()
    local result = {}
    for k, data in ipairs(self.datas) do
        result[k] = BinaryTools.pack_record_data(self.columns, data, self.nil_flags[k])
    end
    return table.concat(result)
end

function Batch:fromBinary(binary_string)
    local record_size = self.columns.record_size
    local num_records = math.floor(#binary_string / record_size)
    for k = 0, num_records - 1 do
        local offset = 1 + k * record_size
        local record_bin = binary_string:sub(offset, offset + record_size - 1)
        local data_list, nil_flags = BinaryTools.unpack_record_data(self.columns, record_bin)
        self:add(data_list, nil_flags)
    end
    return num_records
end

return Batch