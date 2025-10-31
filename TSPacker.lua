local M = {}

local TYPE_CONFIGS = {
    timestamp   = { size=4, maxPrecision=0,  defaultValue=0, formatUnsigned="<I4", formatSigned="<i4" },
    shortnumber = { size=2, maxPrecision=15, defaultValue=0, formatUnsigned="<I2", formatSigned="<i2" },
    number      = { size=4, maxPrecision=15, defaultValue=0, formatUnsigned="<I4", formatSigned="<i4" },
    bignumber   = { size=6, maxPrecision=15, defaultValue=0, formatUnsigned="<I6", formatSigned="<i6" },
    hugenumber  = { size=8, maxPrecision=15, defaultValue=0, formatUnsigned="<I8", formatSigned="<i8" },
}

function M.calculateSchema(schema)
    local formatString = ""
    local recordSize = 0
    local columnNames = {}
    for i, col in ipairs(schema) do
        if not col.name then error(string.format("Column %d: 'name' is missing.", i)) end
        if not col.type then error(string.format("Column %d ('%s'): 'type' is missing.", i, col.name)) end
        if i == 1 then
            if col.name ~= "time" then
                error(string.format("Column %d: First column 'name' must be 'time'.", i))
            end
            if col.type ~= "timestamp" then
                error(string.format("Column %d ('time'): 'type' must be 'timestamp'.", i))
            end
            if not col.interval or col.interval <= 0 then
                error(string.format("Column %d ('time'): 'interval' must be a positive number.", i))
            end
            if not col.startTime or col.startTime <= 0 then
                error(string.format("Column %d ('time'): 'startTime' must be a positive number.", i))
            end
            if col.startTime ~= math.floor(col.startTime / col.interval) * col.interval then
                 error(string.format("Column %d ('time'): 'startTime' (%s) must be aligned with (a multiple of) 'interval' (%s).", i, col.startTime, col.interval))
            end
            col.precision = 0
            col.signed = false
        end
        local typeConfig = TYPE_CONFIGS[col.type]
        if not typeConfig then
            error(string.format("Column %d ('%s'): Unsupported type '%s'.", i, col.name, col.type))
        end
        if not col.precision or col.precision < 0 or col.precision > typeConfig.maxPrecision then
            error(string.format("Column %d ('%s'): 'precision' (%s) is invalid, negative, or exceeds max precision for type '%s'.", i, col.name, tostring(col.precision), col.type))
        end
        col.signed = (col.signed ~= false) 
        col.scale = 10 ^ col.precision
        local currentFormat = col.signed and typeConfig.formatSigned or typeConfig.formatUnsigned
        formatString = formatString .. currentFormat
        recordSize = recordSize + typeConfig.size
        columnNames[col.name] = i
    end

    return {
        formatString = formatString,
        recordSize = recordSize,
        schema = schema,
        columnNames = columnNames,
    }
end

function M.createZeroRecord(schemaConfig, timeValue)
    local zeroRecord = {}
    for i, col in ipairs(schemaConfig.schema) do
        if col.name == "time" then
            zeroRecord[i] = timeValue
        else
            local typeConfig = TYPE_CONFIGS[col.type]
            zeroRecord[i] = typeConfig.defaultValue
        end
    end
    return zeroRecord
end

function M.isZeroRecord(record)
    for j = 2, #record do
        if record[j] ~= 0 then
            return false
        end
    end
    return true
end

local function packRecord(schemaConfig, data, extractFunction)
    local valuesToPack = {}
    for i, col in ipairs(schemaConfig.schema) do
        local rawValue = extractFunction(i, data, col)
        if rawValue == nil or type(rawValue) ~= 'number' then
            error(string.format("Column %d ('%s'): Data is missing, nil, or not a number during packing.", i, col.name))
        end
        local packedValue
        if col.type == "timestamp" then
            packedValue = math.floor(rawValue / col.interval)
        else
            packedValue = math.floor(rawValue * col.scale + 0.5)
        end
        table.insert(valuesToPack, packedValue)
    end
    return string.pack(schemaConfig.formatString, table.unpack(valuesToPack)) 
end

function M.packRecordFromMap(schemaConfig, dataMap)
    return packRecord(schemaConfig, dataMap, function(index, data, col) 
        return data[col.name] 
    end)
end

function M.packRecordFromArray(schemaConfig, dataArray)
    return packRecord(schemaConfig, dataArray, function(index, data, col) 
        return data[index] 
    end)
end

function M.unpackRecord(schemaConfig, binaryString)
    local unpackedValues = { string.unpack(schemaConfig.formatString, binaryString) }
    local result = {}
    for i, col in ipairs(schemaConfig.schema) do
        local originalValue
        if col.type == "timestamp" then
            originalValue = unpackedValues[i] * col.interval
        else
            if col.scale == 1 then
                originalValue = unpackedValues[i]
            else
                originalValue = unpackedValues[i] / col.scale
            end
        end
        table.insert(result, originalValue)
    end
    return result
end

return M