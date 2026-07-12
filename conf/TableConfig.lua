local TableConfig = {
    columns = nil,
    block_size = nil,
    rollup_expr = nil,
    parallel_expr = nil,
}

TableConfig.__index = TableConfig

function TableConfig.new(columns, block_size)
    local self = setmetatable({}, TableConfig)
    self.columns = columns
    self.block_size = (block_size or 4 * 1024 * 1024)
    self.rollup_expr = "first(open),max(high),min(low),last(close),sum(volume),sum(quote_volume),sum(taker_buy_volume),sum(taker_buy_quote_volume),sum(count),last(long_short_delta),last(open_interest)"
    self.parallel_expr = "last(close),lr(close),last(open_interest),lr(open_interest)"
    return self
end

return TableConfig