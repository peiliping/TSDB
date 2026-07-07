local MR = {
    column_id = nil,
    column_name = nil,
    map = nil,
    reduce = nil,
}

MR.__index = MR

local DEFAULT_REDUCE = function(v)
    return v
end

function MR.new(column_id, column_name, map, reduce)
    local self = {}
    setmetatable(self, MR)
    self.column_id = column_id
    self.column_name = column_name
    self.map = map
    self.reduce = reduce or DEFAULT_REDUCE
    return self
end

return MR