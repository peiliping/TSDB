local MapReduce = require("aggregate.MapReduce")

local FS = {
    ES = {
        count = { map = function(l, r) return (l or 0) + 1 end },
        first = { map = function(l, r) return l or r end },
        last  = { map = function(l, r) return r end },
        min   = { map = function(l, r) return l and math.min(l, r) or r end },
        max   = { map = function(l, r) return l and math.max(l, r) or r end },
        sum   = { map = function(l, r) return (l or 0) + r end },
        avg = {
            map = function(l, r)
                if not l then
                    return { r, 1 }
                end
                l[1] = l[1] + r
                l[2] = l[2] + 1
                return l
            end,
            reduce = function(v)
                return v[1] / v[2]
            end
        },
        lr = {
            reduce = function(_, rb)
                local size = rb:size()
                local sumx = 0
                local sumy = 0
                local sumxx = 0
                local sumxy = 0
                for i = 1, size do
                    local v = rb:get(i)
                    sumx = sumx + i
                    sumy = sumy + v
                    sumxx = sumxx + i * i
                    sumxy = sumxy + i * v
                end
                if size < 2 then
                    return { 0, 0 }
                end
                local slope = (size * sumxy - sumx * sumy) / (size * sumxx - sumx * sumx)
                local intercept = sumy / size - slope * sumx / size + slope
                local stdDevAcc = 0
                for i = 1, size do
                    local p = i * slope + intercept
                    stdDevAcc = stdDevAcc + math.pow((rb:get(i) - p), 2)
                end
                local stdDev = math.sqrt(stdDevAcc / (size - 1))
                local py = size * slope + intercept
                return { py + 2 * stdDev, py - 2 * stdDev }
            end
        },
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

function FS.scan_reduce(mr_functions, agg_record, column_datas)
    for i, mr in ipairs(mr_functions) do
        agg_record[i + 1] = mr.reduce(agg_record[i + 1], column_datas and column_datas[i + 1])
    end
end

return FS