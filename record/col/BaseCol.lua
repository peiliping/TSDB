local Types = require("record.col.Types")

local Column = {
    name = nil,
    type_name = nil,
    type_def = nil,
    size = nil,
    format = nil,
}
Column.__index = Column

function Column.new(name, type_name)
    local self = {}
    setmetatable(self, Column)
    self.name = name
    self.type_name = type_name
    self.type_def = Types.get(type_name)
    return self
end

function Column:pack_value(val)
    return val
end

function Column:unpack_value(val)
    return val
end

return Column