local ROOT_PATH = os.getenv("TSDB_PATH") or "./"
local DATA_PATH = ROOT_PATH .. "/data/"
package.path = package.path .. ";" .. ROOT_PATH .. "/?.lua"

local Database = require("db.Database")
local DataTable = require("db.DataTable")
local Functions = require("aggregate.Functions")
local Batch = require("record.Batch")

local function check_arg(name, value)
    if not value then
        error("Argument '" .. name .. "' missing or invalid.")
    end
    return value
end

local function handle_stat(args)
    local table_name = args[2]
    local safe = (args[3] and args[3] == "true" or false)
    local db = Database.new(DATA_PATH, table_name, safe)
    local format_str = "| %-50s | %-50s |"
    local line = "====================================================="
    print(string.format(format_str, "Key", "Value"))
    for _, stat in ipairs(db:scan_tables_stat()) do
        print(line .. "=" .. line)
        for _, t in ipairs(stat) do
            print(string.format(format_str, t.key, t.val))
        end
    end
    print(line .. "=" .. line)
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
    ts_table:flush_header(start_time, end_time)
end

local function handle_read(args)
    local table_name = check_arg("table_name", args[2])
    local start_ts = check_arg("start_ts", tonumber(args[3]))
    local end_ts = check_arg("end_ts", tonumber(args[4]))
    local filter_nil = (args[5] and args[5] == "true" or false)
    local db = Database.new(DATA_PATH, table_name)
    local ts_table = db:get_table(table_name)
    local group = ts_table:query_group(start_ts, end_ts, filter_nil)
    local cache = {}
    for record in group:iterator() do
        print(record:to_string(cache))
    end
end

local function handle_write(args)
    local table_name = check_arg("table_name", args[2])
    local db = Database.new(DATA_PATH, table_name)
    local ts_table = db:get_table(table_name)
    local columns_size = ts_table.columns:count()
    local batch = Batch.new(ts_table.columns, false)
    local arg_size = #args - 2 -- args[1] is "write", args[2] is table name
    if arg_size > 0 then
        if arg_size ~= columns_size then
            error("Args Datas Not Match SchemaSize. Expected " .. columns_size .. ", got " .. arg_size .. ".")
        end
        local record = table.create(columns_size, 0)
        for i = 1, columns_size do
            record[i] = tonumber(args[2 + i])
            if record[i] == nil then
                error("Invalid number format for argument " .. (2 + i) .. ": " .. args[2 + i])
            end
        end
        batch:add(record)
        print(ts_table:write_records(batch))
    else
        local count = 0
        while true do
            local line = io.stdin:read('*l')
            if line == nil then
                break
            end
            local record = table.create(columns_size, 0)
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
            count = count + 1
            if count >= DataTable.LIMIT_SIZE then
                print(ts_table:write_records(batch))
                batch = Batch.new(ts_table.columns, false, DataTable.LIMIT_SIZE)
                count = 0
            end
        end
        if count > 0 then
            print(ts_table:write_records(batch))
        end
    end
end

local function handle_agg(args)
    local table_name = check_arg("table_name", args[2])
    local start_ts = check_arg("start_ts", tonumber(args[3]))
    local end_ts = check_arg("end_ts", tonumber(args[4]))
    local num = check_arg("num", tonumber(args[5]))
    local expr = check_arg("expr", args[6])
    local mode = check_arg("mode", args[7])
    local db = Database.new(DATA_PATH, table_name)
    local ts_table = db:get_table(table_name)
    local aggs = Functions.parse_expression(expr, ts_table.columns)
    local cb = function(ring_buffer)
        for i = 1, ring_buffer:size() do
            print(table.concat(ring_buffer:get(i), " "))
        end
    end
    if mode == "Tumbling" then
        ts_table:query_agg_tumbling(start_ts, end_ts, num, aggs, cb)
    elseif mode == "Sliding" then
        ts_table:query_agg_sliding(start_ts, end_ts, num, aggs, cb)
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
    local start_ts = check_arg("start_ts", tonumber(args[4]))
    local end_ts = check_arg("end_ts", tonumber(args[5]))
    local aggs = Functions.parse_expression(src_table.config.rollup_expr, src_table.columns)
    local cb = function(ring_buffer)
        local batch = Batch.new(dest_table.columns, false, ring_buffer:size())
        for i = 1, ring_buffer:size() do
            batch:add(ring_buffer:get(i))
        end
        print(dest_table:write_records(batch))
    end
    src_table:query_agg_tumbling(start_ts, end_ts, dest_table.interval, aggs, cb)
end

local function handle_parallel(args)
    local src_table_name = check_arg("src_table", args[2])
    local dest_table_name = check_arg("dest_table", args[3])
    local src_db = Database.new(DATA_PATH, src_table_name)
    local dest_db = Database.new(DATA_PATH, dest_table_name)
    local src_table = src_db:get_table(src_table_name)
    local dest_table = dest_db:get_table(dest_table_name)
    local start_ts = check_arg("start_ts", tonumber(args[4]))
    local end_ts = check_arg("end_ts", tonumber(args[5]))
    local size = check_arg("size", tonumber(args[6]))
    local aggs = Functions.parse_expression(src_table.config.parallel_expr, src_table.columns)
    local cb = function(ring_buffer)
        local batch = Batch.new(dest_table.columns, false, ring_buffer:size())
        for i = 1, ring_buffer:size() do
            batch:add(ring_buffer:get(i))
        end
        print(dest_table:write_records(batch))
    end
    src_table:query_agg_sliding(start_ts, end_ts, size, aggs, cb)
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
        usage = "tsdb read <table_name> <start_ts> <end_ts> [<filter_nil>]",
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