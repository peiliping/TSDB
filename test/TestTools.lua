local TestTools = {}

function TestTools.assert_error_msg_contains(expected_msg, func)
    local success, err = pcall(func)
    assert(not success, "Expected an error, but no error occurred.")
    assert(string.find(err, expected_msg), "Error message '" .. err .. "' does not contain '" .. expected_msg .. "'")
end

return TestTools