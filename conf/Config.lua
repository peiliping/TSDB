local S = {}

local TABLE_CONFIG = {
    columns = nil,
    block_size = nil,
    rollup_expr = nil,
    parallel_expr = nil,
}

TABLE_CONFIG.__index = TABLE_CONFIG

function TABLE_CONFIG.new(columns, block_size)
    local self = setmetatable({}, TABLE_CONFIG)
    self.columns = columns
    self.block_size = (block_size or 4 * 1024 * 1024)
    self.rollup_expr = "first(open),max(high),min(low),last(close),sum(volume),sum(quote_volume),sum(taker_buy_volume),sum(taker_buy_quote_volume),sum(count),last(long_short_delta),last(open_interest)"
    self.parallel_expr = "last(close),lr(close),last(open_interest),lr(open_interest)"
    return self
end

local function create_kline_base(interval_sec)
    local kline_base_columns = {
        { name = "time", type = "timestamp", interval = interval_sec },
        { name = "open", type = "number", precision = 2, signed = false },
        { name = "high", type = "number", precision = 2, signed = false },
        { name = "low", type = "number", precision = 2, signed = false },
        { name = "close", type = "number", precision = 2, signed = false },
        { name = "volume", type = "bignumber", precision = 3, signed = false },
        { name = "quote_volume", type = "bignumber", precision = 3, signed = false },
        { name = "taker_buy_volume", type = "bignumber", precision = 3, signed = false },
        { name = "taker_buy_quote_volume", type = "bignumber", precision = 3, signed = false },
        { name = "count", type = "number", precision = 0, signed = false },
        { name = "long_short_delta", type = "shortnumber", precision = 3, signed = true },
        { name = "open_interest", type = "number", precision = 3, signed = false },
    }
    return TABLE_CONFIG.new(kline_base_columns)
end

local function create_kline_ln(interval_sec)
    local kline_ln_columns = {
        { name = "time", type = "timestamp", interval = interval_sec },
        { name = "close", type = "number", precision = 2, signed = false },
        { name = "close_ln_up", type = "number", precision = 2, signed = false },
        { name = "close_ln_down", type = "number", precision = 2, signed = false },
        { name = "open_interest", type = "number", precision = 3, signed = false },
        { name = "oi_ln_up", type = "number", precision = 3, signed = false },
        { name = "oi_ln_down", type = "number", precision = 3, signed = false },
    }
    return TABLE_CONFIG.new(kline_ln_columns)
end

S.BTC_KL_5M = create_kline_base(300)
S.BTC_KL_15M = create_kline_base(900)
S.BTC_KL_1H = create_kline_base(3600)
S.BTC_KL_4H = create_kline_base(14400)

S.BTC_LN_5M = create_kline_ln(300)
S.BTC_LN_15M = create_kline_ln(900)
S.BTC_LN_1H = create_kline_ln(3600)
S.BTC_LN_4H = create_kline_ln(14400)

return S