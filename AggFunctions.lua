local M = {}

local FUNCTIONS = {

	count = function(l, r) return (l or 0) + 1 end,
	first = function(l, r) return l or r end,
	last  = function(l, r) return r end,
    min   = function(l, r) return l and math.min(l, r) or r end,
    max   = function(l, r) return l and math.max(l, r) or r end,
    sum   = function(l, r) return (l or 0) + r end,
}

function M.parser(item)
	local aggType, columnName = string.match(item, "([a-zA-Z]+)%s*%(%s*([^)]+)%s*%)")
	if not columnName or not aggType then
    	error(string.format("Invalid aggregation expression: '%s'. Expected format: aggType(columnName)", item))
    end
	local aggFunction = FUNCTIONS[aggType]
	if not aggFunction then
		error("Invalid aggType : " .. aggType)
	end
	return { columnName = columnName, aggFunction = aggFunction }
end

return M