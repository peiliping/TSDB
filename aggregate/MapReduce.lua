local MR = {
    column_id = nil,
    column_name = nil,
    map = nil,
    reduce = nil,
}

MR.__index = MR

local DEFAULT_MAP = function(tmp_result, current_value)
    return current_value
end

local DEFAULT_REDUCE = function(tmp_result, column_datas)
    return tmp_result
end

function MR.new(column_id, column_name, map, reduce)
    local self = setmetatable({}, MR)
    self.column_id = column_id
    self.column_name = column_name
    self.map = map or DEFAULT_MAP
    self.reduce = reduce or DEFAULT_REDUCE
    return self
end

return MR