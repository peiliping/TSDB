local S = {}

local TABLE_CONFIG = {
    columns = nil,
    block_size = nil,
}

local function create_kline_base(interval_sec)
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
    return { columns = kline_base_columns }
end

S.BTC_KL_5M = create_kline_base(300)
S.BTC_KL_15M = create_kline_base(900)
S.BTC_KL_1H = create_kline_base(3600)
S.BTC_KL_4H = create_kline_base(14400)

return S