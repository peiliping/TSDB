local M = {}

local TYPE_CONFIGS = {
    timestamp   = { size = 4, maxPrecision = 0,  formatUnsigned = "<I4", formatSigned = "<i4" },
    tinynumber  = { size = 1, maxPrecision = 0,  formatUnsigned = "<I1", formatSigned = "<i1" },
    shortnumber = { size = 2, maxPrecision = 4,  formatUnsigned = "<I2", formatSigned = "<i2" },
    number      = { size = 4, maxPrecision = 8,  formatUnsigned = "<I4", formatSigned = "<i4" },
    bignumber   = { size = 6, maxPrecision = 16, formatUnsigned = "<I6", formatSigned = "<i6" },
    hugenumber  = { size = 8, maxPrecision = 16, formatUnsigned = "<I8", formatSigned = "<i8" },
}

function M.calculateSchema(schema)
    local formatString = ""
    local recordSize = 0
    local columnNames = {}
    for i, col in ipairs(schema.columns) do
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

    schema.formatString = formatString
    schema.recordSize = recordSize
    schema.columnNames = columnNames
    schema.columnsSize = #schema.columns
end

function M.createZeroRecord(schema, timeValue)
    local zeroRecord = {}
    for i, col in ipairs(schema.columns) do
        if i == 1 then
            zeroRecord[i] = timeValue
        else
            zeroRecord[i] = 0
        end
    end
    return zeroRecord
end

function M.createZeroRecordBin(schema, timeValue)
    local zeroRecord = M.createZeroRecord(schema, timeValue)
    return M.packRecord(schema, zeroRecord)
end

function M.getZeroRecordBinDataPart(schema)
    local bin = M.createZeroRecordBin(schema, 0)
    return bin:sub(TYPE_CONFIGS["timestamp"].size + 1)
end

function M.isZeroRecord(recordBin, zeroRecordBin)
    local s, e = recordBin:find(zeroRecordBin, TYPE_CONFIGS["timestamp"].size + 1, true)
    return s and e and e > 0
end

function M.packRecord(schema, record)
    for i, col in ipairs(schema.columns) do
        local rawValue = record[i]
        if rawValue == nil or type(rawValue) ~= 'number' then
            error(string.format("Column %d ('%s'): Data is missing, nil, or not a number during packing.", i, col.name))
        end
        if col.type == "timestamp" then
            record[i] = math.floor(rawValue / col.interval)
        else
            record[i] = math.floor(rawValue * col.scale + 0.5)
        end
    end
    return string.pack(schema.formatString, table.unpack(record)) 
end

function M.unpackRecord(schema, binaryString)
    local record = { string.unpack(schema.formatString, binaryString) }
    record[schema.columnsSize + 1] = nil
    for i, col in ipairs(schema.columns) do
        if col.type == "timestamp" then
            record[i] = record[i] * col.interval
        else
            if col.scale ~= 1 then
                record[i] = record[i] / col.scale
            end
        end
    end
    return record
end

return M