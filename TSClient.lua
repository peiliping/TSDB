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

local function executeAgg(tsTable, startTs, endTs, newInterval, exps)
    local aggs = AggFunctions.parserExpr(tsTable.schema, exps)
    local records
    local estimatedRows = math.floor((endTs - startTs) / tsTable.interval)
    if estimatedRows <= 1024 then
        records = tsTable:queryRangeAggV1(startTs, endTs, newInterval, aggs)
    else
        records = tsTable:queryRangeAggV2(startTs, endTs, newInterval, aggs)
    end
    for _, record in ipairs(records) do
        print(table.concat(record, " "))
    end
end

local function executeRollup(srcTable, destTable, startTs, endTs)    
    local aggs = AggFunctions.parserExpr(srcTable.schema) 
    local records = srcTable:queryRangeAggV2(startTs, endTs, destTable.interval, aggs)
    print(destTable:writeRecords(records))
end

local function executeWrite(tsTable, args)
    local columnsSize = tsTable.schema.columnsSize
    local argSize = #args - 2
    if argSize > 0 then
        if argSize ~= columnsSize then error("Args Datas Not Match SchemaSize.")   end
        local record = {}
        for i = 1, columnsSize do
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
            if valueCount ~= columnsSize then
                error(string.format("Stdin Datas Incomplete: Expected %d columns, got %d.", columnsSize, valueCount))
            end
            count = count + 1
            records[count] = record
            if count % 8000 == 0 then
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
    local cmd = args[1]
    if cmd == "stat" then
        local db = TSDB.new(DATA_PATH, args[2], true)
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
        local tb = checkArg("tablenName", args[2])
        local st = checkArg("startTime", tonumber(args[3]))
        local et = checkArg("endTime", tonumber(args[4]))
        local filterZero = (args[5] and args[5]=="true" or false)
        local db = TSDB.new(DATA_PATH, tb, true)
        local tsTable = db:getTable(tb)
        executeQuery(tsTable, st, et, filterZero)
    elseif cmd == "agg" then
        local tb = checkArg("tablenName", args[2])
        local st = checkArg("startTime", tonumber(args[3]))
        local et = checkArg("endTime", tonumber(args[4]))
        local nitvl = checkArg("newInterval", tonumber(args[5]))
        local exps = checkArg("aggItem", args[6])
        local db = TSDB.new(DATA_PATH, tb, true)
        local tsTable = db:getTable(tb)
        executeAgg(tsTable, st, et, nitvl, exps)
    elseif cmd == "write" then
        local tb = checkArg("tablenName", args[2])
        local db = TSDB.new(DATA_PATH, tb, false)
        local tsTable = db:getTable(tb)
        executeWrite(tsTable, args)
    elseif cmd == "rollup" then
        local srcTableName = checkArg("sourceTable", args[2])
        local destTableName = checkArg("destTable", args[3])
        local srcDB = TSDB.new(DATA_PATH, srcTableName, true)
        local destDB = TSDB.new(DATA_PATH, destTableName, false)
        local srcTable = srcDB:getTable(srcTableName)
        local destTable = destDB:getTable(destTableName)
        local st = checkArg("startTime", tonumber(args[4]))
        local et = checkArg("endTime", tonumber(args[5]))
        executeRollup(srcTable, destTable, st, et)
    else
        print("  lua TSClient.lua stat [<table_name>]")
        print("  lua TSClient.lua read <table_name> <start_ts> <end_ts> [filterZero=true]")
        print("  lua TSClient.lua agg <table_name> <start_ts> <end_ts> <new_interval> \"<agg_exps>\"")
        print("  lua TSClient.lua write <table_name> [<data...>] (data from args or stdin)")
        print("  lua TSClient.lua rollup <source_table> <dest_table> <start_ts> <end_ts>")
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