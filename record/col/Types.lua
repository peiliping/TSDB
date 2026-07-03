local T = {}

local TYPES = {
    timestamp   = { size = 4, max_precision = 0,  format_unsigned = "<I4", format_signed = "<i4" },
    tinynumber  = { size = 1, max_precision = 0,  format_unsigned = "<I1", format_signed = "<i1" },
    shortnumber = { size = 2, max_precision = 4,  format_unsigned = "<I2", format_signed = "<i2" },
    number      = { size = 4, max_precision = 8,  format_unsigned = "<I4", format_signed = "<i4" },
    bignumber   = { size = 8, max_precision = 16, format_unsigned = "<I8", format_signed = "<i8" },
}

function T.get(type)
    return TYPES[type] or error("Unknown type: " .. tostring(type))
end

return T