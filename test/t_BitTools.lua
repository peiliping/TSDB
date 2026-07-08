local BitTools = require("tools.BitTools")

local test_case = {}

function test_case.test_set_bit()
    assert(BitTools.set_bit(0, 0) == 1, "set_bit(0, 0) should be 1")
    assert(BitTools.set_bit(0, 1) == 2, "set_bit(0, 1) should be 2")
    assert(BitTools.set_bit(1, 1) == 3, "set_bit(1, 1) should be 3")
    assert(BitTools.set_bit(2, 0) == 3, "set_bit(2, 0) should be 3")
    assert(BitTools.set_bit(5, 3) == 13, "set_bit(5, 3) should be 13 (0101 -> 1101)")
end

function test_case.test_clear_bit()
    assert(BitTools.clear_bit(1, 0) == 0, "clear_bit(1, 0) should be 0")
    assert(BitTools.clear_bit(2, 1) == 0, "clear_bit(2, 1) should be 0")
    assert(BitTools.clear_bit(3, 0) == 2, "clear_bit(3, 0) should be 2")
    assert(BitTools.clear_bit(3, 1) == 1, "clear_bit(3, 1) should be 1")
    assert(BitTools.clear_bit(13, 3) == 5, "clear_bit(13, 3) should be 5 (1101 -> 0101)")
end

function test_case.test_check_bit()
    assert(BitTools.check_bit(1, 0) == true, "check_bit(1, 0) should be true")
    assert(BitTools.check_bit(2, 1) == true, "check_bit(2, 1) should be true")
    assert(BitTools.check_bit(3, 0) == true, "check_bit(3, 0) should be true")
    assert(BitTools.check_bit(3, 1) == true, "check_bit(3, 1) should be true")
    assert(BitTools.check_bit(4, 2) == true, "check_bit(4, 2) should be true")

    assert(BitTools.check_bit(0, 0) == false, "check_bit(0, 0) should be false")
    assert(BitTools.check_bit(1, 1) == false, "check_bit(1, 1) should be false")
    assert(BitTools.check_bit(2, 0) == false, "check_bit(2, 0) should be false")
    assert(BitTools.check_bit(4, 0) == false, "check_bit(4, 0) should be false")
end

function test_case.test_calculate_nil_record_flags()
    assert(BitTools.calculate_nil_record_flags(1) == 0, "size 1 should be 0 (0)") -- 只有时间戳，不能为nil
    assert(BitTools.calculate_nil_record_flags(2) == 2, "size 2 should be 2 (10)") -- 第0位是时间戳，第1位可为nil
    assert(BitTools.calculate_nil_record_flags(3) == 6, "size 3 should be 6 (110)") -- 第0位是时间戳，第1、2位可为nil
    assert(BitTools.calculate_nil_record_flags(4) == 14, "size 4 should be 14 (1110)") -- 第0位是时间戳，第1、2、3位可为nil
end

function test_case.test_calculate_nil_flags()
    assert(BitTools.calculate_nil_flags({}, 0) == 0, "empty list should be 0")
    assert(BitTools.calculate_nil_flags({ "a", "b", "c" }, 3) == 0, "list with no nils should be 0")
    assert(BitTools.calculate_nil_flags({ nil, "b", "c" }, 3) == 1, "first element nil should be 1 (001)")
    assert(BitTools.calculate_nil_flags({ "a", nil, "c" }, 3) == 2, "second element nil should be 2 (010)")
    assert(BitTools.calculate_nil_flags({ "a", "b", nil }, 3) == 4, "third element nil should be 4 (100)")
    assert(BitTools.calculate_nil_flags({ nil, nil, "c" }, 3) == 3, "first two elements nil should be 3 (011)")
    assert(BitTools.calculate_nil_flags({ nil, "b", nil }, 3) == 5, "first and third elements nil should be 5 (101)")
    assert(BitTools.calculate_nil_flags({ "a", nil, nil }, 3) == 6, "second and third elements nil should be 6 (110)")
    assert(BitTools.calculate_nil_flags({ nil, nil, nil }, 3) == 7, "all elements nil should be 7 (111)")
end

return test_case