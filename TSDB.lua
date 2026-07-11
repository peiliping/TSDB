local ROOT_PATH = os.getenv("TSDB_PATH") or "./"
local DATA_PATH = ROOT_PATH .. "/data/"
package.path = package.path .. ";" .. ROOT_PATH .. "/?.lua"

local Database = require("db.Database")
local Functions = require("aggregate.Functions")
local Tools = require("tools.Tools")
local Batch = require("record.Batch")
local Record = require("record.Record")

local function check_arg(name, value)
    if not value then
        error("Argument '" .. name .. "' missing or invalid.")
    end
    return value
end

local function execute_query(ts_table, start_ts, end_ts, filter_zero)
    local group = ts_table:query_group(start_ts, end_ts, 10000, filter_zero)
    local cache = {}
    for record in group:iterator() do
        print(record:to_string(cache))
    end
end

local function execute_agg_tumbling(ts_table, start_ts, end_ts, new_interval, expr)
    local aggs = Functions.parse_expression(expr, ts_table.columns)
    local records = ts_table:query_agg_tumbling(start_ts, end_ts, new_interval, aggs)
    Tools.print_table(records)
end

local function execute_agg_sliding(ts_table, start_ts, end_ts, sliding_size, expr)
    local aggs = Functions.parse_expression(expr, ts_table.columns)
    local records = ts_table:query_agg_sliding(start_ts, end_ts, sliding_size, aggs)
    Tools.print_table(records)
end

local function execute_rollup(src_table, dest_table, start_ts, end_ts)
    local aggs = Functions.parse_expression(src_table.config.rollup_expr, src_table.columns)
    local records = src_table:query_agg_tumbling(start_ts, end_ts, dest_table.interval, aggs)
    local batch = Batch.new(dest_table.columns, false)
    batch:add_datas(records)
    print(dest_table:write_records(batch))
end

local function execute_parallel(src_table, dest_table, start_ts, end_ts, size)
    local aggs = Functions.parse_expression(src_table.config.parallel_expr, src_table.columns)
    local records = src_table:query_agg_sliding(start_ts, end_ts, size, aggs)
    local batch = Batch.new(dest_table.columns, false)
    batch:add_datas(records)
    print(dest_table:write_records(batch))
end

local function execute_write(ts_table, args)
    local columns_size = ts_table.columns:count()
    local batch = Batch.new(ts_table.columns, false)
    local arg_size = #args - 2 -- args[1] is "write", args[2] is table name
    if arg_size > 0 then
        if arg_size ~= columns_size then
            error("Args Datas Not Match SchemaSize. Expected " .. columns_size .. ", got " .. arg_size .. ".")
        end
        local record = {}
        for i = 1, columns_size do
            record[i] = tonumber(args[2 + i])
            if record[i] == nil then
                error("Invalid number format for argument " .. (2 + i) .. ": " .. args[2 + i])
            end
        end
        batch:add(record)
        print(ts_table:write_records(batch))
    else
        while true do
            local line = io.stdin:read('*l')
            if line == nil then
                break
            end
            if #line > 1024 then
                error("Stdin Line Data Too Long.")
            end
            local record = {}
            local value_count = 0
            for value in string.gmatch(line, "[^%s]+") do
                value_count = value_count + 1
                record[value_count] = tonumber(value)
                if record[value_count] == nil then
                    error("Invalid number format in stdin line: '" .. line .. "' for value '" .. value .. "'")
                end
            end
            if value_count ~= columns_size then
                error(string.format("Stdin Datas Incomplete: Expected %d columns, got %d in line: '%s'.", columns_size, value_count, line))
            end
            batch:add(record)
        end
        print(ts_table:write_records(batch))
    end
end

local function handle_stat(args)
    local table_name = args[2]
    local db = Database.new(DATA_PATH, table_name, args[3])
    local format_str = "| %-50s | %-50s |"
    local line = "====================================================="
    print(string.format(format_str, "Key", "Value"))
    for tbl_name, tbl in pairs(db.data_tables) do
        print(line .. "=" .. line)
        print(string.format(format_str, "TableName", tbl_name))
        local stat = tbl:get_stat()
        if stat == nil then
            print(string.format(format_str, "Status", "Not-Ready"))
        else
            for key, value in pairs(stat) do
                print(string.format(format_str, key, value))
            end
        end
    end
end

local function handle_create(args)
    local table_name = check_arg("table_name", args[2])
    local db = Database.new(DATA_PATH, table_name)
    local ts_table = db:get_table(table_name)
    ts_table:create()
end

local function handle_fix(args)
    local table_name = check_arg("table_name", args[2])
    local start_time = check_arg("start_ts", tonumber(args[3]))
    local end_time = check_arg("end_ts", tonumber(args[4]))
    local db = Database.new(DATA_PATH, table_name, true)
    local ts_table = db:get_table(table_name)
    ts_table:savior(start_time, end_time)
end

local function handle_read(args)
    local table_name = check_arg("table_name", args[2])
    local start_time = check_arg("start_ts", tonumber(args[3]))
    local end_time = check_arg("end_ts", tonumber(args[4]))
    local filter_zero = (args[5] and args[5] == "true" or false)
    local db = Database.new(DATA_PATH, table_name)
    local ts_table = db:get_table(table_name)
    execute_query(ts_table, start_time, end_time, filter_zero)
end

local function handle_agg(args)
    local table_name = check_arg("table_name", args[2])
    local start_time = check_arg("start_ts", tonumber(args[3]))
    local end_time = check_arg("end_ts", tonumber(args[4]))
    local num = check_arg("num", tonumber(args[5]))
    local expr = check_arg("expr", args[6])
    local mode = check_arg("mode", args[7])
    local db = Database.new(DATA_PATH, table_name)
    local ts_table = db:get_table(table_name)
    if mode == "Tumbling" then
        execute_agg_tumbling(ts_table, start_time, end_time, num, expr)
    elseif mode == "Sliding" then
        execute_agg_sliding(ts_table, start_time, end_time, num, expr)
    else
        error("Unsupported aggregation mode: '" .. mode .. "'. Must be 'Tumbling' or 'Sliding'.")
    end
end

local function handle_rollup(args)
    local src_table_name = check_arg("src_table", args[2])
    local dest_table_name = check_arg("dest_table", args[3])
    local src_db = Database.new(DATA_PATH, src_table_name)
    local dest_db = Database.new(DATA_PATH, dest_table_name)
    local src_table = src_db:get_table(src_table_name)
    local dest_table = dest_db:get_table(dest_table_name)
    local start_time = check_arg("start_ts", tonumber(args[4]))
    local end_time = check_arg("end_ts", tonumber(args[5]))
    execute_rollup(src_table, dest_table, start_time, end_time)
end

local function handle_parallel(args)
    local src_table_name = check_arg("src_table", args[2])
    local dest_table_name = check_arg("dest_table", args[3])
    local src_db = Database.new(DATA_PATH, src_table_name, true)
    local dest_db = Database.new(DATA_PATH, dest_table_name, false)
    local src_table = src_db:get_table(src_table_name)
    local dest_table = dest_db:get_table(dest_table_name)
    local start_time = check_arg("start_ts", tonumber(args[4]))
    local end_time = check_arg("end_ts", tonumber(args[5]))
    local size = check_arg("size", tonumber(args[6]))
    execute_parallel(src_table, dest_table, start_time, end_time, size)
end

local function handle_write(args)
    local table_name = check_arg("table_name", args[2])
    local db = Database.new(DATA_PATH, table_name)
    local ts_table = db:get_table(table_name)
    execute_write(ts_table, args)
end

--------------------------------------------------------------

local COMMANDS = {
    {
        cmd = "stat",
        handler = handle_stat,
        usage = "tsdb stat [<table_name>] [<safe>]",
        description = "Display statistics for all tables or a specific table."
    },
    {
        cmd = "create",
        handler = handle_create,
        usage = "tsdb create <table_name>",
        description = "Create table by touching file and writing headers."
    },
    {
        cmd = "fix",
        handler = handle_fix,
        usage = "tsdb fix <table_name> <start_ts> <end_ts>",
        description = "Fix table header flush start_ts and end_ts."
    },
    {
        cmd = "read",
        handler = handle_read,
        usage = "tsdb read <table_name> <start_ts> <end_ts> [<filter_zero>]",
        description = "Read records from a table within a timestamp range."
    },
    {
        cmd = "write",
        handler = handle_write,
        usage = "tsdb write <table_name> [<data...>]",
        description = "Write records to a table. Data can be provided as arguments or via stdin."
    },
    {
        cmd = "agg",
        handler = handle_agg,
        usage = "tsdb agg <table_name> <start_ts> <end_ts> <number> <agg_expr> <mode>",
        description = "Perform a Tumbling or Sliding window aggregation and print results."
    },
    {
        cmd = "rollup",
        handler = handle_rollup,
        usage = "tsdb rollup <source_table> <dest_table> <start_ts> <end_ts>",
        description = "Perform a rollup aggregation from a source table to a destination table."
    },
    {
        cmd = "parallel",
        handler = handle_parallel,
        usage = "tsdb parallel <source_table> <dest_table> <start_ts> <end_ts> <size>",
        description = "Perform a parallel (sliding window) aggregation from a source table to a destination table."
    },
}

local function main(args)
    local cmd = args[1]
    if not cmd then
        print("Usage:")
        print("")
        for _, item in ipairs(COMMANDS) do
            print("  - " .. item.usage)
            print("")
            print("      " .. item.description)
            print("")
        end
        return
    end
    local command
    for _, item in ipairs(COMMANDS) do
        if item.cmd == cmd then
            command = item
            break
        end
    end
    if not command then
        print("Unknown command : " .. cmd)
        return
    end
    command.handler(args)
end

if arg then
    xpcall(function()
        main(arg)
    end, function(err)
        io.stderr:write("Operation failed: " .. tostring(err) .. "\n")
        os.exit(1)
    end)
end