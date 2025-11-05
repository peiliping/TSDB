local M = {}

function M.calculate(source)
    local size = #source
    local sumx = 0
    local sumy = 0
    local sumxx = 0
    local sumxy = 0
    for i = 1, size do
        sumx = sumx + i
        sumy = sumy + source[i]
        sumxx = sumxx + i * i
        sumxy = sumxy + i * source[i]
    end
    local slope = (size * sumxy - sumx * sumy) / (size * sumxx - sumx * sumx)
    local intercept = sumy / size - slope * sumx / size + slope
    local stdDevAcc = 0
    for i = 1, size do
        local p = i * slope + intercept
        stdDevAcc = stdDevAcc + math.pow((source[i] - p), 2)
    end
    local stdDev = math.sqrt(stdDevAcc / (size -1))
    local py = size * slope + intercept 
    return { py, py + 2 * stdDev, py - 2 * stdDev }
end

function M.calUp(source)
    local result = M.calculate(source)
    return result[2]
end

function M.calMiddle(source)
    local result = M.calculate(source)
    return result[1]
end

function M.calDown(source)
    local result = M.calculate(source)
    return result[3]
end

return M