local M = {}

local FUNCTIONS = {

	count = function(l, r) return (l or 0) + 1 end,
	first = function(l, r) return l or r end,
	last  = function(l, r) return r end,
    min   = function(l, r) return l and math.min(l, r) or r end,
    max   = function(l, r) return l and math.max(l, r) or r end,
    sum   = function(l, r) return (l or 0) + r end,
}

function M.parserItem(schema, item)
	local aggType, columnName = string.match(item, "([a-zA-Z]+)%s*%(%s*([^)]+)%s*%)")
	if not columnName or not aggType then
    	error(string.format("Invalid aggregation expression: '%s'. Expected format: aggType(columnName)", item))
    end
    if not schema.columnNames[columnName] then
        error(string.format("Column name '%s' not found in schema.", columnName))
    end
	local aggFunction = FUNCTIONS[aggType]
	if not aggFunction then
		error("Invalid aggType : " .. aggType)
	end
	return { columnName = columnName, aggFunction = aggFunction }
end

function M.parserExpr(schema, expression)
	local aggs = {}
	expression = expression or schema.aggExpr
	if not expression then
		error("Schema aggExpr missing.")
	end
	for expItem in string.gmatch(expression, "[^,]+") do
		table.insert(aggs, M.parserItem(schema, expItem))
	end
	return aggs
end

return M