local BASE_PATH = (os.getenv("TSDB_BASE_PATH") or "/root/tsdb")
local DATA_PATH = BASE_PATH .. "/data/"
package.path = package.path .. ";" .. BASE_PATH .. "/?.lua"

local AggFunctions = require("aggregate.Functions")
local DatabaseCore = require("db.Database")

local TSDB = {}

function TSDB.new(...)
    return DatabaseCore.new(...)
end

function TSDB:get_table(...)
    return DatabaseCore:get_table(...)
end

function TSDB:scan_tables_stat(...)
    return DatabaseCore:scan_tables_stat(...)
end

-- Helper function for argument validation
local function checkArg(argName, value)
    if not value then
        error("Argument '" .. argName .. "' missing or invalid.")
    end
    return value
end

-- Command execution functions (kept as is, or slightly modified to be more generic)
local function executeQuery(tsTable, startTs, endTs, filterZero)
    local records = tsTable:queryRange(startTs, endTs, filterZero)
    for _, record in ipairs(records) do
        print(table.concat(record, " "))
    end
end

local function executeRollup(srcTable, destTable, startTs, endTs)
    local aggs = AggFunctions.parserExpr(srcTable.schema, srcTable.schema.rollupExpr)
    local records = srcTable:queryAggTumbling(startTs, endTs, destTable.interval, aggs)
    print(destTable:writeRecords(records))
end

local function executeParallel(srcTable, destTable, startTs, endTs, size)
    local aggs = AggFunctions.parserExpr(srcTable.schema, srcTable.schema.parallelExpr)
    local records = srcTable:queryAggSliding(startTs, endTs, size, aggs)
    print(destTable:writeRecords(records))
end

local function executeAggTumbling(tsTable, startTs, endTs, newInterval, expr)
    local aggs = AggFunctions.parserExpr(tsTable.schema, expr)
    local records = tsTable:queryAggTumbling(startTs, endTs, newInterval, aggs)
    for _, record in ipairs(records) do
        print(table.concat(record, " "))
    end
end

local function executeAggSliding(tsTable, startTs, endTs, slidingSize, expr)
    local aggs = AggFunctions.parserExpr(tsTable.schema, expr)
    -- The original code had `tsTable:queryAggSliding(tsTable, ...)` which is likely a typo.
    -- Assuming the first argument should be `self` (tsTable) and not passed twice.
    local records = tsTable:queryAggSliding(startTs, endTs, slidingSize, aggs)
    for _, record in ipairs(records) do
        print(table.concat(record, " "))
    end
end

local function executeWrite(tsTable, args)
    local columnsSize = tsTable.schema.columnsSize
    local argSize = #args - 2 -- args[1] is "write", args[2] is table name
    if argSize > 0 then
        if argSize ~= columnsSize then
            error("Args Datas Not Match SchemaSize. Expected " .. columnsSize .. ", got " .. argSize .. ".")
        end
        local record = {}
        for i = 1, columnsSize do
            record[i] = tonumber(args[2 + i])
            if record[i] == nil then
                error("Invalid number format for argument " .. (2 + i) .. ": " .. args[2 + i])
            end
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
            if #line > 1024 then
                error("Stdin Line Data Too Long.")
            end
            local record = {}
            local valueCount = 0
            for value in string.gmatch(line, "[^%s]+") do
                valueCount = valueCount + 1
                record[valueCount] = tonumber(value)
                if record[valueCount] == nil then
                    error("Invalid number format in stdin line: '" .. line .. "' for value '" .. value .. "'")
                end
            end
            if valueCount ~= columnsSize then
                error(string.format("Stdin Datas Incomplete: Expected %d columns, got %d in line: '%s'.", columnsSize, valueCount, line))
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

-- Command handler functions
local function handleStat(args)
    local tableName = args[2]
    local db = DatabaseCore.new(DATA_PATH, tableName, true)
    local result = db:scanTablesStat(tableName) -- Pass tableName to scanTablesStat if it's optional
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
end

local function handleRead(args)
    local tb = checkArg("tableName", args[2])
    local st = checkArg("startTime", tonumber(args[3]))
    local et = checkArg("endTime", tonumber(args[4]))
    local filterZero = (args[5] and args[5] == "true" or false)
    local db = DatabaseCore.new(DATA_PATH, tb, true)
    local tsTable = db:get_table(tb)
    executeQuery(tsTable, st, et, filterZero)
end

local function handleWrite(args)
    local tb = checkArg("tableName", args[2])
    local db = DatabaseCore.new(DATA_PATH, tb, false)
    local tsTable = db:get_table(tb)
    executeWrite(tsTable, args)
end

local function handleRollup(args)
    local srcTableName = checkArg("sourceTable", args[2])
    local destTableName = checkArg("destTable", args[3])
    local srcDB = DatabaseCore.new(DATA_PATH, srcTableName, true)
    local destDB = DatabaseCore.new(DATA_PATH, destTableName, false)
    local srcTable = srcDB:get_table(srcTableName)
    local destTable = destDB:get_table(destTableName)
    local st = checkArg("startTime", tonumber(args[4]))
    local et = checkArg("endTime", tonumber(args[5]))
    executeRollup(srcTable, destTable, st, et)
end

local function handleParallel(args)
    local srcTableName = checkArg("sourceTable", args[2])
    local destTableName = checkArg("destTable", args[3])
    local srcDB = DatabaseCore.new(DATA_PATH, srcTableName, true)
    local destDB = DatabaseCore.new(DATA_PATH, destTableName, false)
    local srcTable = srcDB:get_table(srcTableName)
    local destTable = destDB:get_table(destTableName)
    local st = checkArg("startTime", tonumber(args[4]))
    local et = checkArg("endTime", tonumber(args[5]))
    local size = checkArg("size", tonumber(args[6]))
    executeParallel(srcTable, destTable, st, et, size)
end

local function handleAgg(args)
    local tb = checkArg("tableName", args[2])
    local st = checkArg("startTime", tonumber(args[3]))
    local et = checkArg("endTime", tonumber(args[4]))
    local num = checkArg("number", tonumber(args[5]))
    local expr = checkArg("expr", args[6])
    local mode = checkArg("mode", args[7])
    local db = DatabaseCore.new(DATA_PATH, tb, true)
    local tsTable = db:get_table(tb)
    if mode == "Tumbling" then
        executeAggTumbling(tsTable, st, et, num, expr)
    elseif mode == "Sliding" then
        executeAggSliding(tsTable, st, et, num, expr)
    else
        error("Unsupported aggregation mode: '" .. mode .. "'. Must be 'Tumbling' or 'Sliding'.")
    end
end

-- Command dispatch table
local commands = {
    stat = {
        handler = handleStat,
        usage = "tsdb stat [<table_name>]",
        description = "Display statistics for all tables or a specific table."
    },
    read = {
        handler = handleRead,
        usage = "tsdb read <table_name> <start_ts> <end_ts> [filter_zero]",
        description = "Read records from a table within a timestamp range."
    },
    write = {
        handler = handleWrite,
        usage = "tsdb write <table_name> [<data...>]",
        description = "Write records to a table. Data can be provided as arguments or via stdin."
    },
    rollup = {
        handler = handleRollup,
        usage = "tsdb rollup <source_table> <dest_table> <start_ts> <end_ts>",
        description = "Perform a rollup aggregation from a source table to a destination table."
    },
    parallel = {
        handler = handleParallel,
        usage = "tsdb parallel <source_table> <dest_table> <start_ts> <end_ts> <size>",
        description = "Perform a parallel (sliding window) aggregation from a source table to a destination table."
    },
    agg = {
        handler = handleAgg,
        usage = "tsdb agg <table_name> <start_ts> <end_ts> <number> <agg_expr> <mode>",
        description = "Perform a tumbling or sliding window aggregation and print results."
    },
}

local function main(args)
    local cmdName = args[1]

    if not cmdName or not commands[cmdName] then
        print("Usage:")
        for name, cmd in pairs(commands) do
            print("  " .. cmd.usage .. " - " .. cmd.description)
        end
        if cmdName then
            error("Unknown command: '" .. cmdName .. "'")
        end
        return
    end

    local command = commands[cmdName]
    command.handler(args)
end

if arg then
    xpcall(function()
        main(arg)
    end,
            function(err)
                io.stderr:write("Operation failed: " .. tostring(err) .. "\n")
                os.exit(1)
            end
    )
end

return TSDB