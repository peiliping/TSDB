local T = {}

function T.print_table(source)
    if type(source) ~= "table" then
        return
    end
    for _, row in ipairs(source) do
        print(table.concat(row, " "))
    end
end

function T.result_to_batch(source, batch)
    if type(source) ~= "table" then
        return
    end
    for _, row in ipairs(source) do
        batch:add(row)
    end
end

return T