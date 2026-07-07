local MapReduce = require("aggregate.MapReduce")

local FS = {
    ES = {
        count = { map = function(l, r) return (l or 0) + 1 end },
        first = { map = function(l, r) return l or r end },
        last  = { map = function(l, r) return r end },
        min   = { map = function(l, r) return l and math.min(l, r) or r end },
        max   = { map = function(l, r) return l and math.max(l, r) or r end },
        sum   = { map = function(l, r) return (l or 0) + r end },
        avg   = {
            map = function(l, r) l = (l or { 0, 0 }); l[1] = l[1] + r; l[2] = l[2] + 1; return l end,
            reduce = function(v) return v[1] / v[2] end
        }
    }
}

function FS.get(mr_name)
    return FS.ES[mr_name] or error("Unknown MR : " .. tostring(mr_name))
end

function FS.parse_item(expression, columns)
    local mr_name, column_name = string.match(expression, "([%a%d_]+)%s*%(%s*([%a%d_]+)%s*%)")
    if not mr_name or not column_name then
        error(string.format("Invalid expression: '%s'.", expression))
    end
    local mr_function = FS.get(mr_name)
    local column_id = columns:get_index_by_name(column_name)
    return MapReduce.new(column_id, column_name, mr_function.map, mr_function.reduce)
end

function FS.parse_expression(expression, columns)
    local mr_functions = {}
    if not expression then
        error("Expression missing.")
    end
    for expr_item in string.gmatch(expression, "[^,]+") do
        table.insert(mr_functions, FS.parse_item(expr_item, columns))
    end
    return mr_functions
end

function FS.scan_map(mr_functions, agg_record, record)
    for i, mr in ipairs(mr_functions) do
        agg_record[i + 1] = mr.map(agg_record[i + 1], record:get_value_by_index(mr.column_id))
    end
end

function FS.scan_reduce(mr_functions, agg_record)
    for i, mr in ipairs(mr_functions) do
        agg_record[i + 1] = mr.reduce(agg_record[i + 1])
    end
end

return FS