local RingBuffer = require("aggregate.RingBuffer")

local test_case = {}

function test_case.test_new_valid_size()
    local rb = RingBuffer.new(5)
    assert(rb ~= nil, "RingBuffer should be created")
    assert(rb.max_size == 5, "max_size should be 5")
    assert(rb.index == 0, "index should be 0 initially")
    assert(rb:size() == 0, "size should be 0 initially")
    assert(not rb:is_full(), "should not be full initially")
end

function test_case.test_new_invalid_size()
    local status, err = pcall(RingBuffer.new, 0)
    assert(not status, "Should fail with max_size = 0")
    assert(string.find(err, "max_size must be positive."), "Error message should indicate invalid size")

    status, err = pcall(RingBuffer.new, -1)
    assert(not status, "Should fail with negative max_size")
    assert(string.find(err, "max_size must be positive."), "Error message should indicate invalid size")

    status, err = pcall(RingBuffer.new, nil)
    assert(not status, "Should fail with nil max_size")
    assert(string.find(err, "max_size must be positive."), "Error message should indicate invalid size")
end

function test_case.test_add_and_size()
    local rb = RingBuffer.new(3)
    assert(rb:size() == 0, "Initial size should be 0")

    rb:add("a")
    assert(rb:size() == 1, "Size should be 1 after adding one element")
    assert(rb:get(1) == "a", "First element should be 'a'")

    rb:add("b")
    assert(rb:size() == 2, "Size should be 2 after adding two elements")
    assert(rb:get(2) == "b", "Second element should be 'b'")

    rb:add("c")
    assert(rb:size() == 3, "Size should be 3 after adding three elements")
    assert(rb:get(3) == "c", "Third element should be 'c'")
    assert(rb:is_full(), "RingBuffer should be full")

    -- Test overwrite
    rb:add("d")
    assert(rb:size() == 3, "Size should still be 3 after overwrite")
    assert(rb:get(1) == "b", "First element should now be 'b'")
    assert(rb:get(2) == "c", "Second element should now be 'c'")
    assert(rb:get(3) == "d", "Third element should now be 'd'")

    rb:add("e")
    assert(rb:size() == 3, "Size should still be 3 after another overwrite")
    assert(rb:get(1) == "c", "First element should now be 'c'")
    assert(rb:get(2) == "d", "Second element should now be 'd'")
    assert(rb:get(3) == "e", "Third element should now be 'e'")
end

function test_case.test_get_elements()
    local rb = RingBuffer.new(3)
    rb:add("a")
    rb:add("b")
    rb:add("c")

    assert(rb:get(1) == "a", "Get first element")
    assert(rb:get(2) == "b", "Get second element")
    assert(rb:get(3) == "c", "Get third element")

    -- Test get after overwrite
    rb:add("d") -- 'a' is overwritten
    assert(rb:get(1) == "b", "Get first element after overwrite")
    assert(rb:get(2) == "c", "Get second element after overwrite")
    assert(rb:get(3) == "d", "Get third element after overwrite")

    rb:add("e") -- 'b' is overwritten
    assert(rb:get(1) == "c", "Get first element after second overwrite")
    assert(rb:get(2) == "d", "Get second element after second overwrite")
    assert(rb:get(3) == "e", "Get third element after second overwrite")
end

function test_case.test_get_out_of_range()
    local rb = RingBuffer.new(3)
    rb:add("a")
    rb:add("b")

    local status, err = pcall(rb.get, rb, 0)
    assert(not status, "Should fail with index 0")
    assert(string.find(err, "Index out of range."), "Error message should indicate out of range")

    status, err = pcall(rb.get, rb, 3)
    assert(not status, "Should fail with index > size")
    assert(string.find(err, "Index out of range."), "Error message should indicate out of range")

    status, err = pcall(rb.get, rb, 4)
    assert(not status, "Should fail with index > max_size")
    assert(string.find(err, "Index out of range."), "Error message should indicate out of range")
end

function test_case.test_is_full()
    local rb = RingBuffer.new(2)
    assert(not rb:is_full(), "Should not be full initially")
    rb:add(1)
    assert(not rb:is_full(), "Should not be full after one element")
    rb:add(2)
    assert(rb:is_full(), "Should be full after two elements")
    rb:add(3)
    assert(rb:is_full(), "Should still be full after overwrite")
end

function test_case.test_clear()
    local rb = RingBuffer.new(3)
    rb:add("a")
    rb:add("b")
    rb:add("c")
    assert(rb:size() == 3, "Size should be 3 before clear")
    assert(rb:is_full(), "Should be full before clear")

    rb:clear()
    assert(rb:size() == 0, "Size should be 0 after clear")
    assert(not rb:is_full(), "Should not be full after clear")
    local status, err = pcall(rb.get, rb, 1)
    assert(not status, "Should fail to get element after clear")
    assert(string.find(err, "Index out of range."), "Error message should indicate out of range")

    -- Add elements after clear
    rb:add("x")
    assert(rb:size() == 1, "Size should be 1 after adding element to cleared buffer")
    assert(rb:get(1) == "x", "Element should be 'x'")
end

return test_case