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
            map = function(cache, v)
                if not cache then
                    cache = {sumx = 0, sumy = 0, sumxx = 0, sumxy = 0, seq = 0}
                end
                cache.seq = cache.seq + 1
                cache.sumx = cache.sumx + cache.seq
                cache.sumy = cache.sumy + v
                cache.sumxx = cache.sumxx + cache.seq * cache.seq
                cache.sumxy = cache.sumxy + cache.seq * v
                return cache
            end,
            reduce = function(cache, rb)
                local size = rb:size()
                if size < 2 then
                    return { 0, 0 }
                end
                local sumx = cache.sumx
                local sumy = cache.sumy
                local sumxx = cache.sumxx
                local sumxy = cache.sumxy
                local slope = (size * sumxy - sumx * sumy) / (size * sumxx - sumx * sumx)
                local intercept = sumy / size - slope * sumx / size + slope
                local stdDevAcc = 0
                for i = 1, size do
                    local p = i * slope + intercept
                    local d = rb:get(i) - p
                    stdDevAcc = stdDevAcc + d * d
                end
                local stdDev = math.sqrt(stdDevAcc / (size - 1))
                local py = size * slope + intercept
                return { py + 2 * stdDev, py - 2 * stdDev }
            end,
            result_size = 2,
        },
    }
}

function FS.get(mr_name)
    return FS.ES[mr_name] or error("Unknown MR : " .. tostring(mr_name))
end

function FS.parse_item(expression, columns, result_id)
    local mr_name, column_name = string.match(expression, "([%a%d_]+)%s*%(%s*([%a%d_]+)%s*%)")
    if not mr_name or not column_name then
        error(string.format("Invalid expression: '%s'.", expression))
    end
    local mr_function = FS.get(mr_name)
    local column_id = columns:get_index_by_name(column_name)
    return MapReduce.new(column_id, column_name, mr_function.map, mr_function.reduce, result_id, mr_function.result_size)
end

function FS.parse_expression(expression, columns)
    local mr_functions = {}
    if not expression then
        error("Expression missing.")
    end
    local result_id = 2
    for expr_item in string.gmatch(expression, "[^,]+") do
        local mr = FS.parse_item(expr_item, columns, result_id)
        result_id = result_id + mr.result_size
        table.insert(mr_functions, mr)
    end
    return mr_functions
end

return FS