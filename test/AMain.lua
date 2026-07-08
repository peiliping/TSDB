local t_Functions = require("test.t_Functions")

local test_cases = { t_Functions }

for i, tc in ipairs(test_cases) do
    print("Running " .. i .. "...")
    for k, fc in pairs(tc) do
        if type(fc) == "function" and string.sub(k, 1, 4) == "test" then
            print("===== " .. k .. " =====")
            fc()
            print("Passed.")
        end
    end
end