local M = {}

local function create_kline_schema(interval_sec, startTime_sec)
    
    local kline_base_schema = {
        { name = "time", type = "timestamp", interval = interval_sec},
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
    return { schema = kline_base_schema }
end

M.BTCUSDT_5MIN  = create_kline_schema(300)
--M.BTCUSDT_15MIN = create_kline_schema(900)
--M.BTCUSDT_30MIN = create_kline_schema(1800)
--M.BTCUSDT_1H    = create_kline_schema(3600)
--M.BTCUSDT_4H    = create_kline_schema(14400)

return M