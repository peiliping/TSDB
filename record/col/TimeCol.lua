local BaseCol = require("record.col.BaseCol")

local TimeCol = {
    interval = nil,
}
TimeCol.__index = TimeCol
setmetatable(TimeCol, { __index = BaseCol })

function TimeCol.new(id, name, interval)
    local self = setmetatable(BaseCol.new(id, name, "timestamp"), TimeCol)
    if not interval or interval <= 0 then
        error(string.format("Column ('%s'): 'interval' must be a positive number.", name))
    end
    self.interval = interval
    self.size = self.type_def.size
    self.format = self.type_def.format_unsigned
    return self
end

function TimeCol:pack_value(val)
    return math.floor(val / self.interval)
end

function TimeCol:unpack_value(val)
    return val * self.interval
end

return TimeCol