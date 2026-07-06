local MR = {
    column_id = nil,
    column_name = nil,
    map = nil,
    reduce = nil,
}

MR.__index = MR

function MR.new(column_id, column_name, map, reduce)
    local self = {}
    setmetatable(self, MR)
    self.column_id = column_id
    self.column_name = column_name
    self.map = map
    self.reduce = reduce
    return self
end

return MR