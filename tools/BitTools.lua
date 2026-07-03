local B = {}

function B.set_bit(flags, pos)
    return flags | (1 << pos)
end

function B.clear_bit(flags, pos)
    return flags & (~(1 << pos))
end

function B.check_bit(flags, pos)
    return (flags & (1 << pos)) ~= 0
end

function B.calculate_nil_record_flags(size)
    return (1 << size) - 1 - 1
end

function B.calculate_nil_flags(data_list)
    local flags = 0
    for i, data in ipairs(data_list) do
        if data == nil then
            flags = B.set_bit(flags, i - 1)
        end
    end
    return flags
end

return B