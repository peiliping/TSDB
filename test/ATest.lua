local test_cases = {
    { require("test.t_Functions") },
    { require("test.t_RingBuffer") },
    { require("test.t_BitTools") },
    { require("test.t_BinaryTools") },
    --TODO @record
    --TODO @db
}

for i, tc in ipairs(test_cases) do
    print("Testing : " .. tc[2] .. "...")
    for k, fc in pairs(tc[1]) do
        if type(fc) == "function" and string.sub(k, 1, 4) == "test" then
            print("test method : " .. k)
            fc()
            print("test result : Passed.")
        end
    end
    print("--------------------------------------------------------------------------------")
end