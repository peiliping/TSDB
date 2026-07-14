local ConfigToColumns = require("conf.ConfigToColumns")
local DataFile = require("db.DataFile")
local Record = require("record.Record")
local Batch = require("record.Batch")
local GroupBatch = require("record.GroupBatch")
local RingBuffer = require("aggregate.RingBuffer")

local DataTable = {
    LIMIT_SIZE = 10000,
    name = nil,
    config = nil,
    columns = nil,
    data_file = nil,
    initialized = nil,
    interval = nil,
}

DataTable.__index = DataTable

function DataTable.new(name, config, file_path, safe)
    local self = setmetatable({}, DataTable)
    self.name = name
    self.config = config
    self.columns = ConfigToColumns.convert(config)
    self.data_file = DataFile.new(file_path, config.block_size, self.columns:get_interval(), self.columns.record_size)
    self.initialized = self.data_file:exist()
    if self.initialized then
        if safe then
            self.data_file:safe_load(function(bin)
                return Record.from_binary(self.columns, bin):get_timestamp()
            end)
        else
            self.data_file:load()
        end
    end
    self.interval = self.data_file.interval
    return self
end

function DataTable:create()
    if not self.initialized then
        self.data_file:create()
        self.data_file:load()
        self.initialized = true
    end
end

function DataTable:flush_header(start_time, end_time)
    self.data_file:flush_header(start_time, end_time)
end

function DataTable:get_stat()
    if self.initialized then
        return {
            { key = "interval", val = self.data_file.interval },
            { key = "start_time", val = self.data_file.start_time },
            { key = "end_time", val = self.data_file.end_time },
            { key = "record_size", val = self.data_file.record_size },
            { key = "file_size", val = self.data_file.file_size },
            { key = "estimated_rows", val = self.data_file:count() },
        }
    end
    return nil
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
    if (aligned_end - aligned_start) / self.interval > DataTable.LIMIT_SIZE then
        error("Time Range is too large.")
    end
    self.data_file:read(batch, aligned_start, aligned_end)
    return batch
end

function DataTable:query_group(start_time, end_time, filter_nil)
    self:_check_init()
    local aligned_start = align_to_interval(start_time, self.interval)
    local aligned_end = align_to_interval(end_time, self.interval)
    return GroupBatch.new(self, aligned_start, aligned_end, DataTable.LIMIT_SIZE, filter_nil)
end

local function scan_reduce(mr_functions, agg_record, column_datas)
    for i = #mr_functions, 1, -1 do
        local mr = mr_functions[i]
        local result = mr.reduce(agg_record[i + 1], column_datas and column_datas[mr.column_id])
        if mr.result_size == 1 then
            agg_record[mr.result_id] = result
        else
            for j = 1, mr.result_size do
                agg_record[mr.result_id + j - 1] = result[j]
            end
        end
    end
end

function DataTable:query_agg_tumbling(start_time, end_time, agg_interval, mr_functions, callback)
    self:_check_init()
    local group = self:query_group(start_time, end_time, true)
    local result = RingBuffer.new(DataTable.LIMIT_SIZE)
    local cur_agg_time
    local last_agg_time
    local agg_record
    for record in group:iterator() do
        cur_agg_time = align_to_interval(record:get_timestamp(), agg_interval)
        if cur_agg_time ~= last_agg_time then
            if result:is_full() then
                callback(result)
                result:clear()
            end
            if agg_record then
                scan_reduce(mr_functions, agg_record)
            end
            agg_record = { cur_agg_time }
            result:add(agg_record)
            last_agg_time = cur_agg_time
        end
        for i = 1, #mr_functions do
            local mr = mr_functions[i]
            agg_record[i + 1] = mr.map(agg_record[i + 1], record:get_value_by_index(mr.column_id))
        end
    end
    if agg_record then
        scan_reduce(mr_functions, agg_record)
    end
    if result:size() > 0 then
        callback(result)
        result:clear()
    end
end

function DataTable:query_agg_sliding(start_time, end_time, sliding_size, mr_functions, callback)
    self:_check_init()
    local group = self:query_group(start_time, end_time, true)
    local result = RingBuffer.new(DataTable.LIMIT_SIZE)
    local column_datas = {}
    local col_count = self.columns:count()
    for i = 1, col_count do
        column_datas[i] = RingBuffer.new(sliding_size)
    end
    local seq = 0
    for record in group:iterator() do
        seq = seq + 1
        for i = 1, col_count do
            column_datas[i]:add(record:get_value_by_index(i))
        end
        if seq >= sliding_size then
            if result:is_full() then
                callback(result)
                result:clear()
            end
            local agg_record = { record:get_timestamp() }
            result:add(agg_record)
            for i = 1, sliding_size do
                for k = 1, #mr_functions do
                    local mr = mr_functions[k]
                    agg_record[k + 1] = mr.map(agg_record[k + 1], column_datas[mr.column_id]:get(i))
                end
            end
            scan_reduce(mr_functions, agg_record, column_datas)
        end
    end
    if result:size() > 0 then
        callback(result)
        result:clear()
    end
end

return DataTable