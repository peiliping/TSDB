local BASE_PATH = (os.getenv("TSDB_BASE_PATH") or "/root/tsdb")
local DATA_PATH = BASE_PATH .. "/data/"
package.path = package.path .. ";" .. BASE_PATH .. "/?.lua"

local AggFunctions = require("AggFunctions")
local TSDB = require("TSDatabase")

local function executeQuery(tsTable, startTs, endTs, filterZero)
    local records = tsTable:queryRange(startTs, endTs, filterZero)
    for _, record in ipairs(records) do
        print(table.concat(record, " "))
    end
end

local function executeAgg(tsTable, startTs, endTs, newInterval, args)
    local columnNames = tsTable.config.columnNames
    local aggs = {}
    for idx = 6, #args do
        local aggItem = AggFunctions.parser(args[idx])
        if not columnNames[aggItem.columnName] then
            error(string.format("Column name '%s' not found in schema for this table.", aggItem.columnName))
        end
        aggs[idx - 5] = aggItem
    end
    local records
    local estimatedRows = math.floor((endTs - startTs) / tsTable.config.schema[1].interval)
    if estimatedRows <= 1024 then
        records = tsTable:queryRangeAggV1(startTs, endTs, newInterval, aggs)
    else
        records = tsTable:queryRangeAggV2(startTs, endTs, newInterval, aggs)
    end
    for _, record in ipairs(records) do
        print(table.concat(record, " "))
    end
end

local function executeWrite(tsTable, args)
    local schema = tsTable.config.schema
    local schemaSize = #schema
    local argSize = #args - 2
    if argSize > 0 then
        if argSize ~= schemaSize then error("Args Datas Not Match SchemaSize.")   end
        local record = {}
        for i = 1, schemaSize do
            record[i] = tonumber(args[2 + i])
        end
        print(tsTable:writeRecords({ record }))
    else
        local records = {}
        local count = 0
        local totalResult = 0

        while true do
            local line = io.stdin:read('*l')
            if line == nil then
                break
            end
            if #line > 1024 then error("Stdin Line Data Too Long.") end
            local record = {}
            local valueCount = 0
            for value in string.gmatch(line, "[^%s]+") do
                valueCount = valueCount + 1
                record[valueCount] = tonumber(value)
            end
            if valueCount ~= schemaSize then
                error("Stdin Datas Incomplete.")
            end
            count = count + 1
            records[count] = record
            if count % 4000 == 0 then
                totalResult = totalResult + tsTable:writeRecords(records)
                count = 0
                records = {}
            end
        end
        print(totalResult + tsTable:writeRecords(records))
    end
end

local function checkArg(key, value)
    if not value then
        error("Arg " .. key .. " missing.")
    end
    return value
end

local function main(args)
    local cmd = checkArg("CMD", args[1])
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
    elseif cmd == "read" then
        checkArg("tablenName", tableName)
        local st = checkArg("startTime", tonumber(args[3]))
        local et = checkArg("endTime", tonumber(args[4]))
        local filterZero = (args[5] and args[5]=="true" or false)
        local db = TSDB.new(DATA_PATH, tableName, true)
        local tsTable = db:getTable(tableName)
        executeQuery(tsTable, st, et, filterZero)
    elseif cmd == "agg" then
        checkArg("tablenName", tableName)
        local st = checkArg("startTime", tonumber(args[3]))
        local et = checkArg("endTime", tonumber(args[4]))
        local nitvl = checkArg("newInterval", tonumber(args[5]))
        checkArg("aggItem", args[6])
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