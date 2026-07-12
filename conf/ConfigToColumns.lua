local Columns = require("record.col.Columns")
local TimeCol = require("record.col.TimeCol")
local NumberCol = require("record.col.NumberCol")

local CTC = {}

function CTC.convert(config)
    local cols = {}
    for i, column in ipairs(config.columns) do
        if not column.name then
            error(string.format("Column %d: 'name' is missing.", i))
        end
        if not column.type then
            error(string.format("Column %d ('%s'): 'type' is missing.", i, column.name))
        end
        if i == 1 then
            if column.type ~= "timestamp" then
                error(string.format("Column %d ('time'): 'type' must be 'timestamp'.", i))
            end
            table.insert(cols, TimeCol.new(column.name, column.interval))
        else
            table.insert(cols, NumberCol.new(column.name, column.type, column.precision, column.signed))
        end
    end
    return Columns.new(cols)
end

return CTC