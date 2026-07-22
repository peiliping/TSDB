local MR = {
    column_id = nil,
    column_name = nil,
    map = nil,
    reduce = nil,
    result_id = nil,
    result_size = nil,
}

MR.__index = MR

local DEFAULT_MAP = function(tmp_result, current_value)
    return current_value
end

local DEFAULT_REDUCE = function(tmp_result, column_datas)
    return tmp_result
end

function MR.new(column_id, column_name, map, reduce, result_id, result_size)
    local self = setmetatable({}, MR)
    self.column_id = column_id
    self.column_name = column_name
    self.map = map or DEFAULT_MAP
    self.reduce = reduce or DEFAULT_REDUCE
    self.result_id = result_id
    self.result_size = result_size or 1
    return self
end

return MR