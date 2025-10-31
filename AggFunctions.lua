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
	local aggType, colName = string.match(item, "([a-zA-Z]+)%(([^)]+)%)")
	return colName, FUNCTIONS[aggType]
end

return M