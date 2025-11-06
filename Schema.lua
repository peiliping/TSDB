local M = {}

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
    return { columns = kline_base_columns,
             rollupExpr = "first(open),max(high),min(low),last(close),sum(volume),sum(quote_volume),sum(taker_buy_volume),sum(taker_buy_quote_volume),sum(count),last(long_short_delta),last(open_interest)",
             parallelExpr = "tail(close),lrUP(close),lrDN(close),tail(open_interest),lrUP(open_interest),lrDN(open_interest)",
            }
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
    return { columns = kline_ln_columns }
end

M.BTC_KL_5M  = create_kline_base(300)
M.BTC_KL_15M = create_kline_base(900)
M.BTC_KL_30M = create_kline_base(1800)
M.BTC_KL_1H  = create_kline_base(3600)
M.BTC_KL_4H  = create_kline_base(14400)

M.BTC_LN_5M  = create_kline_ln(300)
M.BTC_LN_15M = create_kline_ln(900)
M.BTC_LN_30M = create_kline_ln(1800)
M.BTC_LN_1H  = create_kline_ln(3600)
M.BTC_LN_4H  = create_kline_ln(14400)

return M
