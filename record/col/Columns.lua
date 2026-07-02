local Types = require("record.col.Types")
local BitTools = require("tools.BitTools")

local Columns = {
    cols = nil,
    name_to_index = nil,
    record_size = nil,
    format_string = nil,
    nil_record_flags = nil,
}
Columns.__index = Columns

function Columns.new(columns_list)
    local self = {}
    setmetatable(self, Columns)
    if not columns_list or type(columns_list) ~= "table" then
        error("Columns.new: 'columns_list' must be a table.")
    end
    if #columns_list > 32 then
        error("Columns.new: 'columns_list' size cannot be greater than 32.")
    end
    if #columns_list < 2 then
        error("Columns.new: 'columns_list' must contain at least 2 columns.")
    end
    local first_col = columns_list[1]
    if not first_col or first_col.type_name ~= "timestamp" then
        error("Columns.new: The first column must be of type 'timestamp'.")
    end
    local nil_flags_type = Types.get("number")
    self.cols = columns_list
    self.name_to_index = {}
    self.record_size = nil_flags_type.size
    self.format_string = nil_flags_type.format_unsigned
    self.nil_record_flags = BitTools.calculate_nil_record_flags(#columns_list)
    for i, col in ipairs(columns_list) do
        self.name_to_index[col.name] = i
        self.record_size = self.record_size + col.size
        self.format_string = self.format_string .. col.format
    end
    return self
end

function Columns:get_index_by_name(column_name)
    local index = self.name_to_index[column_name]
    if not index then
        error("Column index not found for name: " .. tostring(column_name))
    end
    return index
end

function Columns:get_by_index(index)
    local col = self.cols[index]
    if not col then
        error("Column not found at index: " .. tostring(index))
    end
    return col
end

function Columns:get_by_name(column_name)
    return self:get_by_index(self:get_index_by_name(column_name))
end

function Columns:count()
    return #self.cols
end

return Columns