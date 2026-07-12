local TableConfig = {
    columns = nil,
    block_size = nil,
    rollup_expr = nil,
    parallel_expr = nil,
}

TableConfig.__index = TableConfig

function TableConfig.new(columns, block_size, rollup_expr, parallel_expr)
    local self = setmetatable({}, TableConfig)
    self.columns = columns
    self.block_size = (block_size or 4 * 1024 * 1024)
    self.rollup_expr = rollup_expr or ""
    self.parallel_expr = parallel_expr or ""
    return self
end

return TableConfig