local BitTools = require("tools.BitTools")
local BinaryTools = require("tools.BinaryTools")
local Columns = require("record.col.Columns")

local Record = {
    columns = nil,
    data = nil,
    nil_flags = nil,
}
Record.__index = Record

function Record.new(columns, data_list, nil_flags)
    local self = setmetatable({}, Record)
    if not columns or type(columns) ~= "table" then
        error("Record.new: 'columns' must be an a table.")
    end
    if not data_list or type(data_list) ~= "table" then
        error("Record.new: 'data_list' must be a table.")
    end
    if columns:count() < #data_list then
        error("Record.new: Column definition count does not match data value count.")
    end
    local ts = data_list[Columns.TIMESTAMP_INDEX]
    local interval = columns:get_interval()
    if ts % interval ~= 0 then
        error("Data Time not match interval.")
    end
    self.columns = columns
    self.data = data_list
    if nil_flags then
        self.nil_flags = nil_flags
    else
        self.nil_flags = BitTools.calculate_nil_flags(data_list, columns:count())
    end
    return self
end

function Record.create_nil_record(columns, timestamp_value)
    return Record.new(columns, { timestamp_value }, columns.nil_record_flags)
end

function Record.from_binary(columns, binary_string)
    local data_list, nil_flags = BinaryTools.unpack_record_data(columns, binary_string)
    return Record.new(columns, data_list, nil_flags)
end

function Record:is_nil_record()
    return self.nil_flags == self.columns.nil_record_flags
end

function Record:get_timestamp()
    return self:get_value_by_index(Columns.TIMESTAMP_INDEX)
end

function Record:get_value(column_name)
    return self:get_value_by_index(self.columns:get_index_by_name(column_name))
end

function Record:set_value(column_name, value)
    self:set_value_by_index(self.columns:get_index_by_name(column_name), value)
end

function Record:_check_index(index)
    if index < 1 or index > self.columns:count() then
        error(string.format("Record: Index %d is out of bounds (1 to %d).", index, self.columns:count()))
    end
end

function Record:get_value_by_index(index, default_val)
    self:_check_index(index)
    if BitTools.check_bit(self.nil_flags, index - 1) then
        return default_val
    end
    return self.data[index]
end

function Record:set_value_by_index(index, value)
    self:_check_index(index)
    if value == nil then
        self.nil_flags = BitTools.set_bit(self.nil_flags, index - 1)
    else
        self.nil_flags = BitTools.clear_bit(self.nil_flags, index - 1)
    end
    self.data[index] = value
end

function Record:is_column_nil(column_name)
    return self:is_column_nil_by_index(self.columns:get_index_by_name(column_name))
end

function Record:is_column_nil_by_index(index)
    self:_check_index(index)
    return BitTools.check_bit(self.nil_flags, index - 1)
end

function Record:to_binary()
    return BinaryTools.pack_record_data(self.columns, self.data, self.nil_flags)
end

function Record:to_string(_cache)
    if self.nil_flags == 0 then
        return table.concat(self.data, " ")
    else
        if not _cache then
            _cache = {}
        end
        for i = 1, self.columns:count() do
            _cache[i] = self:get_value_by_index(i, "nil");
        end
        return table.concat(_cache, " ")
    end
end

return Record