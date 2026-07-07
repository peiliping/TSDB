local S = {}

local TABLE_CONFIG = {
    columns = nil,
    block_size = nil,
}

TABLE_CONFIG.__index = TABLE_CONFIG

function TABLE_CONFIG.new(columns, block_size)
    local self = {}
    setmetatable(self, TABLE_CONFIG)
    self.columns = columns
    self.block_size = (block_size or 4 * 1024 * 1024)
    return self
end

local function create_kline_base(interval_sec, block_size)
    local kline_base_columns = {
        { name = "time", type = "timestamp", interval = interval_sec },
        -- K线价格数据
        { name = "open", type = "number", precision = 2, signed = false },
        { name = "high", type = "number", precision = 2, signed = false },
        { name = "low", type = "number", precision = 2, signed = false },
        { name = "close", type = "number", precision = 2, signed = false },
        -- 交易量数据 (使用 bignumber)
        { name = "volume", type = "bignumber", precision = 3, signed = false },
        { name = "quote_volume", type = "bignumber", precision = 3, signed = false },
        { name = "taker_buy_volume", type = "bignumber", precision = 3, signed = false },
        { name = "taker_buy_quote_volume", type = "bignumber", precision = 3, signed = false },
        -- 其他数据
        { name = "count", type = "number", precision = 0, signed = false },
        { name = "long_short_delta", type = "shortnumber", precision = 3, signed = true },
        { name = "open_interest", type = "number", precision = 3, signed = false },
    }
    return TABLE_CONFIG.new(kline_base_columns, block_size)
end

S.BTC_KL_5M = create_kline_base(300)
S.BTC_KL_15M = create_kline_base(900)
S.BTC_KL_1H = create_kline_base(3600)
S.BTC_KL_4H = create_kline_base(14400)

return S