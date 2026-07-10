local T = {}
--TODO
local function flatten(tbl, _idx, _result)
    if type(tbl) ~= "table" then
        error("flatten for not table.")
    end
    if not _idx then
        _idx = 0
    end
    if not _result then
        _result = {}
    end
    for _, v in ipairs(tbl) do
        if type(v) == "table" then
            _idx = flatten(v, _idx, _result)
        else
            _idx = _idx + 1
            _result[_idx] = v
        end
    end
    return _idx, _result
end

function T.print_table(source)
    if type(source) ~= "table" then
        return
    end
    for _, row in ipairs(source) do
        local _, r = flatten(row)
        print(table.concat(r, " "))
    end
end

function T.result_to_batch(source, batch)
    if type(source) ~= "table" then
        return
    end
    for _, row in ipairs(source) do
        local _, r = flatten(row)
        batch:add(r)
    end
end

return T