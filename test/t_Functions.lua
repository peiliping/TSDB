local TestTools = require("test.TestTools")
local Config = require("conf.Config")
local Columns = require("record.col.Columns")
local Functions = require("aggregate.Functions")

local test_case = {}

local real_columns = Columns.from_config(Config.BTC_KL_5M)

function test_case.test_FS_parse_item()
    local mr_func = Functions.parse_item("sum(open)", real_columns, 2)
    assert(mr_func ~= nil)
    assert(mr_func.column_id == real_columns:get_by_name("open").id)
    assert(mr_func.column_name == "open")
    assert(type(mr_func.map) == "function")
    assert(type(mr_func.reduce) == "function")
    assert(mr_func.result_id == 2)
    assert(mr_func.result_size == 1)

    mr_func = Functions.parse_item("avg(close)", real_columns, 2)
    assert(mr_func ~= nil)
    assert(mr_func.column_id == real_columns:get_by_name("close").id)
    assert(mr_func.column_name == "close")
    assert(type(mr_func.map) == "function")
    assert(type(mr_func.reduce) == "function")
    assert(mr_func.result_id == 2)
    assert(mr_func.result_size == 1)

    TestTools.assert_error_msg_contains("Invalid expression: 'sum open'.", function()
        Functions.parse_item("sum open", real_columns)
    end)

    TestTools.assert_error_msg_contains("Column not found with name: non_existent_col", function()
        Functions.parse_item("sum(non_existent_col)", real_columns)
    end)
end

function test_case.test_FS_parse_expression()
    local mr_funcs = Functions.parse_expression("sum(volume)", real_columns)
    assert(#mr_funcs == 1)
    assert(mr_funcs[1].column_id == real_columns:get_by_name("volume").id)
    assert(mr_funcs[1].column_name == "volume")
    assert(mr_funcs[1].result_id == 2)
    assert(mr_funcs[1].result_size == 1)

    mr_funcs = Functions.parse_expression("sum(open),avg(close),max(high)", real_columns)
    assert(#mr_funcs == 3)

    assert(mr_funcs[1].column_id == real_columns:get_by_name("open").id)
    assert(mr_funcs[1].column_name == "open")
    assert(mr_funcs[1].result_id == 2)
    assert(mr_funcs[1].result_size == 1)

    assert(mr_funcs[2].column_id == real_columns:get_by_name("close").id)
    assert(mr_funcs[2].column_name == "close")
    assert(mr_funcs[2].result_id == 3)
    assert(mr_funcs[2].result_size == 1)

    assert(mr_funcs[3].column_id == real_columns:get_by_name("high").id)
    assert(mr_funcs[3].column_name == "high")
    assert(mr_funcs[3].result_id == 4)
    assert(mr_funcs[3].result_size == 1)

    TestTools.assert_error_msg_contains("Unknown MR : unknown_func", function()
        Functions.parse_expression("unknown_func(open)", real_columns)
    end)

    TestTools.assert_error_msg_contains("Invalid expression: 'sum open'.", function()
        Functions.parse_expression("sum open", real_columns)
    end)

    TestTools.assert_error_msg_contains("Column not found with name: non_existent_col", function()
        Functions.parse_expression("sum(open),avg(non_existent_col)", real_columns)
    end)

    TestTools.assert_error_msg_contains("Expression missing.", function()
        Functions.parse_expression(nil, real_columns)
    end)
end

return test_case