local Functions = require("aggregate.Functions")

local test_case = {}

function test_case.test_FS_get()
    -- Test valid function names
    assert(Functions.get("count") ~= nil)
    assert(Functions.get("first") ~= nil)
    assert(Functions.get("last") ~= nil)
    assert(Functions.get("min") ~= nil)
    assert(Functions.get("max") ~= nil)
    assert(Functions.get("sum") ~= nil)
    assert(Functions.get("avg") ~= nil)
    assert(Functions.get("lr") ~= nil)
end

function test_case.test_FS_parse_item()
    local mock_columns = {
        get_index_by_name = function(_, name)
            if name == "col1" then return 1 end
            if name == "col2" then return 2 end
            if name == "col3" then error("Column index not found for name: " .. tostring(name)) end
            return nil
        end
    }

    -- Test valid expression
    local mr_func = Functions.parse_item("sum(col1)", mock_columns)
    assert(mr_func ~= nil)
    assert(mr_func.column_id == 1)
    assert(mr_func.column_name == "col1")
    assert(type(mr_func.map) == "function")
    assert(type(mr_func.reduce) == "function")

    -- Test another valid expression
    mr_func = Functions.parse_item("avg(col2)", mock_columns)
    assert(mr_func ~= nil)
    assert(mr_func.column_id == 2)
    assert(mr_func.column_name == "col2")
    assert(type(mr_func.map) == "function")
    assert(type(mr_func.reduce) == "function")

    -- Test invalid expression format
    local status, err = pcall(Functions.parse_item, "sum col1", mock_columns)
    assert(not status)
    assert(string.find(err, "Invalid expression: 'sum col1'."))

    -- Test unknown column
    status, err = pcall(Functions.parse_item, "sum(col3)", mock_columns)
    assert(not status)
    assert(string.find(err, "Column index not found for name: col3"))
end

return test_case