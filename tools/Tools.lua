local T = {}

function T.print_table(source)
    if type(source) ~= "table" then
        return
    end
    for _, row in ipairs(source) do
        print(table.concat(row, " "))
    end
end

return T