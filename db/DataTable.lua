local DataFile = require("db.DataFile")
local TimeCol = require("record.col.TimeCol")
local NumberCol = require("record.col.NumberCol")
local Columns = require("record.col.Columns")
local Record = require("record.Record")
local Batch = require("record.Batch")
local GroupBatch = require("record.GroupBatch")
local Functions = require("aggregate.Functions")
local RingBuffer = require("aggregate.RingBuffer")

local DataTable = {
    name = nil,
    columns = nil,
    data_file = nil,
    initialized = nil,
    interval = nil,
}

DataTable.__index = DataTable

local function config_to_cols(config)
    local cols = {}
    for i, column in ipairs(config.columns) do
        if not column.name then
            error(string.format("Column %d: 'name' is missing.", i))
        end
        if not column.type then
            error(string.format("Column %d ('%s'): 'type' is missing.", i, column.name))
        end
        if i == 1 then
            if column.type ~= "timestamp" then
                error(string.format("Column %d ('time'): 'type' must be 'timestamp'.", i))
            end
            table.insert(cols, TimeCol.new(column.name, column.interval))
        else
            table.insert(cols, NumberCol.new(column.name, column.type, column.precision, column.signed))
        end
    end
    return cols
end

function DataTable.new(name, config, file_path)
    local self = {}
    setmetatable(self, DataTable)
    self.name = name
    self.columns = Columns.new(config_to_cols(config))
    self.data_file = DataFile.new(file_path, config.block_size,
            self.columns:get_interval(), self.columns.record_size)
    self.initialized = self.data_file:exist()
    if self.initialized then
        self.data_file:load()
    end
    self.interval = self.data_file.interval
    return self
end

function DataTable:create()
    if not self.initialized then
        self.data_file:create()
        self.initialized = true
    end
end

function DataTable:get_stat()
    return (not self.initialized) and nil or {
        start_time = self.data_file.start_time,
        end_time = self.data_file.end_time,
        interval = self.data_file.interval,
        file_size = self.data_file.file_size,
        record_size = self.data_file.record_size,
        estimated_rows = self.data_file:count(),
    }
end

local function align_to_interval(ts, interval)
    return math.floor(ts / interval) * interval
end

function DataTable:_check_init()
    if not self.initialized then
        error("DataTable is not initialized.")
    end
end

function DataTable:write_records(batch)
    self:_check_init()
    if batch:count() == 0 then
        return 0
    end
    local r = 0
    if self.data_file.end_time > 0 and batch:start_time() > self.data_file.end_time + self.interval then
        local blanks = Batch.new(self.columns, false)
        for ts = self.data_file.end_time + self.interval, batch:start_time() - self.interval, self.interval do
            blanks:add_record(Record.create_nil_record(self.columns, ts))
        end
        r = r + self.data_file:write(blanks)
    end
    return r + self.data_file:write(batch)
end

function DataTable:query_records(start_time, end_time, filter_nil)
    self:_check_init()
    local batch = Batch.new(self.columns, filter_nil)
    local aligned_start = align_to_interval(start_time, self.interval)
    local aligned_end = align_to_interval(end_time, self.interval)
    self.data_file:read(batch, aligned_start, aligned_end)
    return batch
end

function DataTable:query_group(start_time, end_time, records_per_batch, filter_nil)
    local aligned_start = align_to_interval(start_time, self.interval)
    local aligned_end = align_to_interval(end_time, self.interval)
    return GroupBatch.new(self, aligned_start, aligned_end, records_per_batch, filter_nil)
end

function DataTable:query_agg_tumbling(start_time, end_time, agg_interval, mr_functions)
    self:_check_init()
    local group = self:query_group(start_time, end_time, nil, true)
    local result = {}
    local cur_agg_time
    local last_agg_time
    local agg_record
    for record in group:iterator() do
        cur_agg_time = align_to_interval(record:getTimestamp(), agg_interval)
        if cur_agg_time ~= last_agg_time then
            if agg_record then
                Functions.scan_reduce(mr_functions, agg_record)
            end
            agg_record = { cur_agg_time }
            table.insert(result, agg_record)
            last_agg_time = cur_agg_time
        end
        for i, mr in ipairs(mr_functions) do
            agg_record[i + 1] = mr.map(agg_record[i + 1], record:get_value_by_index(mr.column_id))
        end
    end
    if agg_record then
        Functions.scan_reduce(mr_functions, agg_record)
    end
    return result
end

function DataTable:query_agg_sliding(start_time, end_time, slidingSize, mr_functions)
    self:_check_init()
    local group = self:query_group(start_time, end_time, nil, true)
    local result = {}
    local column_datas = {}
    local col_count = self.columns:count()
    for i = 1, col_count do
        column_datas[i] = RingBuffer.new(slidingSize)
    end
    local seq = 0
    for record in group:iterator() do
        seq = seq + 1
        for i = 1, col_count do
            column_datas[i]:add(record:get_value_by_index(i))
        end
        if seq >= slidingSize then
            local agg_record = { record:getTimestamp() }
            table.insert(result, agg_record)
            for i = 1, slidingSize do
                for k, mr in ipairs(mr_functions) do
                    agg_record[k + 1] = mr.map(agg_record[k + 1], column_datas[mr.column_id]:get(i))
                end
            end
            Functions.scan_reduce(mr_functions, agg_record, column_datas)
        end
    end
    return result
end

return DataTable