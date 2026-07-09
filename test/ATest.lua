local test_cases = {
    "test.t_Functions",
    "test.t_RingBuffer",
    "test.t_BitTools",
    "test.t_BinaryTools",
    "test.t_Record",
    "test.t_Batch",
    "test.t_DataFile",
    "test.t_DataTable",
}

for _, module_name in ipairs(test_cases) do
    local test_module = require(module_name)
    print("Testing : " .. module_name .. "...")
    local test_methods = {}
    for k, fc in pairs(test_module) do
        if type(fc) == "function" and string.sub(k, 1, 4) == "test" then
            table.insert(test_methods, k)
        end
    end
    table.sort(test_methods)
    for _, method_name in ipairs(test_methods) do
        print("test method : " .. method_name)
        test_module[method_name]()
        print("test result : Passed.")
    end
    print("--------------------------------------------------------------------------------")
end