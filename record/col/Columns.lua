local TimeCol = require("record.col.TimeCol")
local NumberCol = require("record.col.NumberCol")
local BitTools = require("tools.BitTools")

local Columns = {
    NIL_FLAGS_COL = NumberCol.new(0, "nil_flags", "number", 0, false),
    TIMESTAMP_COL = TimeCol.new(1, "timestamp", 1),
    cols = nil,
    record_size = nil,
    format_string = nil,
    nil_record_flags = nil,
}
Columns.__index = Columns

function Columns.new(columns_list)
    local self = setmetatable({}, Columns)
    if not columns_list or type(columns_list) ~= "table" then
        error("Columns.new: 'columns_list' must be a table.")
    end
    if #columns_list > 32 then
        error("Columns.new: 'columns_list' size cannot be greater than 32.")
    end
    if #columns_list < 2 then
        error("Columns.new: 'columns_list' must contain at least 2 columns.")
    end
    local first_col = columns_list[Columns.TIMESTAMP_COL.id]
    if not first_col or first_col.type_name ~= Columns.TIMESTAMP_COL.type_name then
        error("Columns.new: The first column must be 'timestamp'.")
    end
    self.cols = table.create(#columns_list, #columns_list)
    self.record_size = Columns.NIL_FLAGS_COL.size
    self.format_string = Columns.NIL_FLAGS_COL.format
    self.nil_record_flags = BitTools.calculate_nil_record_flags(#columns_list)
    for i, col in ipairs(columns_list) do
        self.cols[i] = col
        self.cols[col.name] = col
        self.record_size = self.record_size + col.size
        self.format_string = self.format_string .. col.format
    end
    return self
end

function Columns.from_config(config)
    local cols = table.create(#config.columns, 0)
    for i, column in ipairs(config.columns) do
        if not column.name then
            error(string.format("Column %d: 'name' is missing.", i))
        end
        if not column.type then
            error(string.format("Column %d ('%s'): 'type' is missing.", i, column.name))
        end
        if column.type == Columns.TIMESTAMP_COL.type_name then
            table.insert(cols, TimeCol.new(i, column.name, column.interval))
        else
            table.insert(cols, NumberCol.new(i, column.name, column.type, column.precision, column.signed))
        end
    end
    return Columns.new(cols)
end

function Columns:get_by_index(index)
    local col = self.cols[index]
    if not col then
        error("Column not found with index: " .. tostring(index))
    end
    return col
end

function Columns:get_by_name(column_name)
    local col = self.cols[column_name]
    if not col then
        error("Column not found with name: " .. tostring(column_name))
    end
    return col
end

function Columns:count()
    return #self.cols
end

function Columns:get_interval()
    return self:get_by_index(Columns.TIMESTAMP_COL.id).interval
end

function Columns:check_timestamp(data_list)
    local interval = self:get_interval()
    local ts = data_list[Columns.TIMESTAMP_COL.id]
    if ts % interval ~= 0 then
        error("Data Time not match interval.")
    end
    return ts, interval
end

return Columns