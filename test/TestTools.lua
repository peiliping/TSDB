local TestTools = {}

-- Helper function for asserting errors
function TestTools.assertErrorMsgContains(expected_msg, func)
    local success, err = pcall(func)
    assert(not success, "Expected an error, but no error occurred.")
    assert(string.find(err, expected_msg), "Error message '" .. err .. "' does not contain '" .. expected_msg .. "'")
end

return TestTools