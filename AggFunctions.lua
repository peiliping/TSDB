local M = {}

local LR = require("LinearRegression")

local FUNCTIONS = {

    count = function(l, r) return (l or 0) + 1 end,
    first = function(l, r) return l or r end,
    last  = function(l, r) return r end,
    min   = function(l, r) return l and math.min(l, r) or r end,
    max   = function(l, r) return l and math.max(l, r) or r end,
    sum   = function(l, r) return (l or 0) + r end,
    ------
    head  = function(source) return source[1] end,
    tail  = function(source) return source[#source] end,
    lrUP  = function(source) return LR.calUp(source) end,
    lrMD  = function(source) return LR.calMiddle(source) end,
    lrDN  = function(source) return LR.calDown(source) end,
}

function M.parserItem(schema, item)
    local aggType, columnName = string.match(item, "([a-zA-Z]+)%s*%(%s*([^)]+)%s*%)")
    if not columnName or not aggType then
        error(string.format("Invalid aggregation expression: '%s'. Expected format: aggType(columnName)", item))
    end
    local columnId = schema.columnNames[columnName]
    if not columnId then
        error(string.format("Column name '%s' not found in schema.", columnName))
    end
    local aggFunction = FUNCTIONS[aggType]
    if not aggFunction then
        error("Invalid aggType : " .. aggType)
    end
    return { columnId = columnId, columnName = columnName, aggFunction = aggFunction }
end

function M.parserExpr(schema, expression)
    local aggs = {}
    if not expression then
        error("Expression missing.")
    end
    for exprItem in string.gmatch(expression, "[^,]+") do
        table.insert(aggs, M.parserItem(schema, exprItem))
    end
    return aggs
end

return M