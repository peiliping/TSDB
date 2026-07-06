local DataFile = require("db.DataFile")
local TimeCol = require("record.col.TimeCol")
local NumberCol = require("record.col.NumberCol")
local Columns = require("record.col.Columns")
local Record = require("record.Record")
local Batch = require("record.Batch")

local DataTable = {
    name = nil,
    columns = nil,
    data_file = nil,
    initialized = nil,
    interval = nil,
}

DataTable.__index = DataTable

local function config2cols(config)
    local cols = {}
    for i, column in ipairs(config.columns) do
        if not column.name then
            error(string.format("Column %d: 'name' is missing.", i))
        end
        if not column.type then
            error(string.format("Column %d ('%s'): 'type' is missing.", i, column.name))
        end
        if i == 1 then
            if column.name ~= "time" then
                error(string.format("Column %d: First column 'name' must be 'time'.", i))
            end
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
    self.columns = Columns.new(config2cols(config))
    self.data_file = DataFile.new(file_path, (config.block_size or 4 * 1024 * 1024),
            self.columns:get_interval(), self.columns.record_size)
    self.initialized = self.data_file:exist()
    if self.initialized then
        self.data_file:load()
    end
    self.interval = self.data_file.interval
    return self
end

function DataTable:getStat()
    return (not self.initialized) and nil or {
        start_time = self.data_file.start_time,
        end_time = self.data_file.end_time,
        interval = self.data_file.interval,
        file_size = self.data_file.file_size,
        record_size = self.data_file.record_size,
        estimated_rows = self.data_file:count(),
    }
end

local function alignToInterval(ts, interval)
    return math.floor(ts / interval) * interval
end

function DataTable:writeRecords(batch)
    if (not self.initialized) or (batch:count() == 0) then
        return 0
    end
    local r = 0
    if batch:start_time() > self.data_file.end_time + self.interval then
        local blanks = Batch.new(self.columns, false)
        for ts = self.data_file.end_time + self.interval, batch:start_time() - self.interval, self.interval do
            blanks:add_record(Record.create_nil_record(self.columns, ts))
        end
        r = r + self.data_file:write(blanks)
    end
    return r + self.data_file:write(batch)
end

function DataTable:queryRecords(start_time, end_time, filter_nil)
    local batch = Batch.new(self.columns, filter_nil)
    if not self.initialized then
        return batch
    end
    local aligned_start = alignToInterval(start_time, self.interval)
    local aligned_end = alignToInterval(end_time, self.interval)
    self.data_file:read(batch, aligned_start, aligned_end)
    return batch
end

function DataTable:queryAggTumbling(start_time, end_time, aggInterval, aggs)

end

function DataTable:queryAggSliding(start_time, end_time, slidingSize, aggs)

end

return DataTable