local BaseCol = require("record.col.BaseCol")

local NumberCol = {
    precision = nil,
    signed = nil,
    -- cache
    scale = nil,
}
NumberCol.__index = NumberCol
setmetatable(NumberCol, {__index = BaseCol})

function NumberCol.new(name, type_name, precision, signed)
    local self = BaseCol.new(name, type_name)
    setmetatable(self, NumberCol)
    local actual_precision = precision or 0
    if actual_precision > self.type_def.max_precision then
        error(string.format("Precision (%d) for column '%s' of type '%s' exceeds its max_precision (%d).",
                            actual_precision, name, type_name, self.type_def.max_precision))
    end
    self.precision = actual_precision
    self.scale = 10 ^ actual_precision
    self.signed = signed or false
    self.size = self.type_def.size
    self.format = self.signed and self.type_def.format_signed or self.type_def.format_unsigned
    return self
end

function NumberCol:pack_value(val)
    return math.floor(val * self.scale + 0.5)
end

function NumberCol:unpack_value(val)
    if self.scale == 1 then
        return val
    else
        return val / self.scale
    end
end

return NumberCol