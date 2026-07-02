local Record = require("record.Record")
local BitTools = require("tools.BitTools")
local BinaryTools = require("tools.BinaryTools")

local Batch = {
    columns = nil,
    datas = nil,
    nil_flags = nil,
}

Batch.__index = Batch

function Batch.new(columns)
    local self = {}
    setmetatable(self, Batch)
    if not columns or type(columns) ~= "table" then
        error("Batch.new: 'columns' must be a table (Columns object).")
    end
    self.columns = columns
    self.datas = {}
    self.nil_flags = {}
    return self
end

function Batch:add(data, nil_flags)
    table.insert(self.datas, data)
    if not nil_flags then
        nil_flags = BitTools.calculate_nil_flags(data)
    end
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

function Batch:fromBinary(binary_string, start_offset, end_offset)
    local record_size = self.columns.record_size
    local start_pos = start_offset or 1
    local end_pos = end_offset or #binary_string
    local num_records = math.floor((end_pos - start_pos + 1) / record_size)
    for k = 0, num_records - 1 do
        local offset = start_pos + (k * record_size)
        local record_bin = binary_string:sub(offset, offset + record_size - 1)
        local data_list, nil_flags = BinaryTools.unpack_record_data(self.columns, record_bin)
        self:add(data_list, nil_flags)
    end
    return num_records
end

return Batch