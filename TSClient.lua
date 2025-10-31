local BASE_PATH = (os.getenv("TSDB_BASE_PATH") or "/root/tsdb")
local DATA_PATH = BASE_PATH .. "/data/"
package.path = package.path .. ";" .. BASE_PATH .. "/?.lua"

local AggFunctions = require("AggFunctions")
local TSDB = require("TSDatabase")

local function executeQuery(tsTable, startTs, endTs, filterZero)
    local results = tsTable:queryRange(startTs, endTs, filterZero)
    if #results > 0 then
        for _, record in ipairs(results) do
            print(table.concat(record, " "))
        end
    end
end

local function executeAgg(tsTable, startTs, endTs, newInterval, args)
    local aggs = {}
    for idx = 6, #args do
        local colName, aggFunction = AggFunctions.parser(args[idx])
        if not colName or not aggFunction then
            error(string.format("Invalid aggregation expression: '%s'. Expected format: aggType(columnName)", args[idx]))
        end
        if not tsTable.config.columnNames[colName] then
            error(string.format("Column name '%s' not found in schema for this table.", colName))
        end
        table.insert(aggs , {columnName = colName, aggFunction = aggFunction})
    end
    local results = tsTable:queryRangeAgg(startTs, endTs, newInterval, aggs)
    if #results > 0 then
        for _, record in ipairs(results) do
            print(table.concat(record, " "))
        end
    end
end

local function executeWrite(tsTable, args)
    local schema = tsTable.config.schema
    local schemaSize = #schema
    if (#args - 2) % schemaSize ~= 0 then
        error("CMD write datas incomplete.")
    end
    local numRecords = (#args - 2) / schemaSize
    local records = {}
    for idx = 1, numRecords do
        local record = {}
        for i, col in ipairs(schema) do
            local value = args[2 + i + (idx - 1) * schemaSize]
            table.insert(record, tonumber(value))
        end
        table.insert(records, record)
    end
    print(tsTable:writeRecords(records))
end

local function checkArg(key, value)
    if not value then
        error("Arg " .. key .. " missing.")
    end
    return value
end

local function main(args)
    local cmd = args[1]
    if not cmd then error("CMD is missing.") end
    local tableName = args[2]

    if cmd == "stat" then
        local db = TSDB.new(DATA_PATH, tableName, true)
        local result = db:scanTablesStat()
        local formatStr = "| %-50s | %-50s |"
        local line = "====================================================="
        for tblName, stat in pairs(result) do
            print(line .. "=" .. line)
            print(string.format(formatStr, "Key", "Value"))
            print(string.format(formatStr, "TableName", tblName))
            for key, value in pairs(stat) do
                print(string.format(formatStr, key, value))
            end
        end
    elseif cmd == "cat" then
        checkArg("tablenName", tableName)
        local st = checkArg("startTime", tonumber(args[3]))
        local et = checkArg("endTime", tonumber(args[4]))
        local db = TSDB.new(DATA_PATH, tableName, true)
        local tsTable = db:getTable(tableName)
        executeQuery(tsTable, st, et, false)
    elseif cmd == "read" then
        checkArg("tablenName", tableName)
        local st = checkArg("startTime", tonumber(args[3]))
        local et = checkArg("endTime", tonumber(args[4]))
        local db = TSDB.new(DATA_PATH, tableName, true)
        local tsTable = db:getTable(tableName)
        executeQuery(tsTable, st, et, true)
    elseif cmd == "agg" then
        checkArg("tablenName", tableName)
        local st = checkArg("startTime", tonumber(args[3]))
        local et = checkArg("endTime", tonumber(args[4]))
        local nitvl = checkArg("newInterval", tonumber(args[5]))
        local db = TSDB.new(DATA_PATH, tableName, true)
        local tsTable = db:getTable(tableName)
        executeAgg(tsTable, st, et, nitvl, args)
    elseif cmd == "write" then
        checkArg("tablenName", tableName)
        local db = TSDB.new(DATA_PATH, tableName, false)
        local tsTable = db:getTable(tableName)
        executeWrite(tsTable, args)
    else
        error("Invalid CMD : " .. cmd)
    end
end

xpcall( function()
            main(arg)
        end,
        function(err)
            io.stderr:write("Operation failed: " .. tostring(err) .. "\n")
            os.exit(1)
        end
)