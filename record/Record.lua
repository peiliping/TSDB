local BitTools = require("tools.BitTools")
local BinaryTools = require("tools.BinaryTools")

local Record = {
    columns = nil,
    data = nil,
    nil_flags = nil,
}
Record.__index = Record

function Record.new(columns, data_list, nil_flags)
    local self = {}
    setmetatable(self, Record)
    if not columns or type(columns) ~= "table" then
        error("Record.new: 'columns' must be an a table.")
    end
    if not data_list or type(data_list) ~= "table" then
        error("Record.new: 'data_list' must be a table.")
    end
    if columns:count() ~= #data_list then
        error("Record.new: Column definition count does not match data value count.")
    end
    self.columns = columns
    self.data = data_list
    self.nil_flags = 0
    if nil_flags then
        self.nil_flags = nil_flags
    else
        self.nil_flags = BitTools.calculate_nil_flags(data_list)
    end
    return self
end

function Record.create_nil_record(columns, timestamp_value)
    local data_list = { timestamp_value }
    for i = 2, columns:count() do
        data_list[i] = nil
    end
    return Record.new(columns, data_list, columns.nil_record_flags)
end

function Record.fromBinary(columns, binary_string)
    local data_list, nil_flags = BinaryTools.unpack_record_data(columns, binary_string)
    return Record.new(columns, data_list, nil_flags)
end

function Record:is_nil_record()
    return self.nil_flags == self.columns.nil_record_flags
end

function Record:get_value(column_name)
    local index = self.columns:get_index_by_name(column_name)
    if not index then
        error("Record:get_value: Column '" .. column_name .. "' not found.")
    end
    if BitTools.check_bit(self.nil_flags, index - 1) then
        return nil
    end
    return self.data[index]
end

function Record:set_value(column_name, value)
    local index = self.columns:get_index_by_name(column_name)
    if not index then
        error("Record:set_value: Column '" .. column_name .. "' not found.")
    end
    if value == nil then
        self.nil_flags = BitTools.set_bit(self.nil_flags, index - 1)
    else
        self.nil_flags = BitTools.clear_bit(self.nil_flags, index - 1)
    end
    self.data[index] = value
end

function Record:get_value_by_index(index)
    if index < 1 or index > self.columns:count() then
        error(string.format("Record:get_value_by_index: Index %d is out of bounds (1 to %d).", index, self.columns:count()))
    end
    if BitTools.check_bit(self.nil_flags, index - 1) then
        return nil
    end
    return self.data[index]
end

function Record:set_value_by_index(index, value)
    if index < 1 or index > self.columns:count() then
        error(string.format("Record:set_value_by_index: Index %d is out of bounds (1 to %d).", index, self.columns:count()))
    end
    if value == nil then
        self.nil_flags = BitTools.set_bit(self.nil_flags, index - 1)
    else
        self.nil_flags = BitTools.clear_bit(self.nil_flags, index - 1)
    end
    self.data[index] = value
end

function Record:is_column_nil(column_name)
    local index = self.columns:get_index_by_name(column_name)
    if not index then
        error("Record:is_column_nil: Column '" .. column_name .. "' not found.")
    end
    return BitTools.check_bit(self.nil_flags, index - 1)
end

function Record:is_column_nil_by_index(index)
    if index < 1 or index > self.columns:count() then
        error(string.format("Record:is_column_nil_by_index: Index %d is out of bounds (1 to %d).", index, self.columns:count()))
    end
    return BitTools.check_bit(self.nil_flags, index - 1)
end

function Record:toBinary()
    return BinaryTools.pack_record_data(self.columns, self.data, self.nil_flags)
end

return Record