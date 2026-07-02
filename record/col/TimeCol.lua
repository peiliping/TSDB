local BaseCol = require("record.col.BaseCol")

local TimeCol = {
    interval = nil,
}
TimeCol.__index = TimeCol
setmetatable(TimeCol, {__index = BaseCol})

function TimeCol.new(name, interval)
    local self = BaseCol.new(name, "timestamp")
    setmetatable(self, TimeCol)
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