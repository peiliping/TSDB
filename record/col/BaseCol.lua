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
    error(string.format("Column:pack_value must be implemented by subclasses for type '%s'.", self.type_name))
end

function Column:unpack_value(val)
    error(string.format("Column:unpack_value must be implemented by subclasses for type '%s'.", self.type_name))
end

return Column