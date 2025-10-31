local M = {}

local TSPacker = require("TSPacker")

local TSTable = {}
TSTable.__index = TSTable

local function getFileSize(filePath)
    local file = io.open(filePath, "rb")
    if not file then return 0 end
    local size = file:seek("end")
    file:close() 
    return size or 0
end

local function alignToInterval(ts, interval)
    return math.floor(ts / interval) * interval
end

function M.new(schema, filePath, readOnly)
    
    local self = {
        config = TSPacker.calculateSchema(schema),
        filePath = filePath,
        fileSize = 0,
        startTime = 0,
        endTime = 0,
        readOnly = readOnly,
    }

    self.startTime = self.config.schema[1].startTime
    local interval = self.config.schema[1].interval
    local recordSize = self.config.recordSize

     ::retry::

    self.fileSize = getFileSize(self.filePath)
    local numFullRecords = math.floor(self.fileSize / recordSize)
    if not readOnly then
        if self.fileSize == 0 then
            local file = io.open(self.filePath, "wb")
            if not file then
                error(string.format("Failed to create and initialize data file: %s", self.filePath))
            end
            local zeroRecord = TSPacker.createZeroRecord(self.config, self.startTime)
            local packedZeroRecord = TSPacker.packRecordFromArray(self.config, zeroRecord)
            file:write(packedZeroRecord)
            file:flush()
            file:close()
            goto retry
        else
            local remainder = self.fileSize % recordSize
            if remainder > 0 then
                local file = io.open(self.filePath, "r+b")
                if not file then
                    error(string.format("Failed to open data file for fixing (remainder > 0): %s", self.filePath))
                end
                local seekOffset = numFullRecords * recordSize
                file:seek("set", seekOffset)
                local nextRecordTime = self.startTime + numFullRecords * interval
                local zeroRecord = TSPacker.createZeroRecord(self.config, nextRecordTime)
                local packedZeroRecord = TSPacker.packRecordFromArray(self.config, zeroRecord)
                file:write(packedZeroRecord)
                file:flush()
                file:close()
                goto retry
            end
        end
    end
    if numFullRecords > 0 then
        self.endTime = self.startTime + (numFullRecords - 1) * interval
    end
    return setmetatable(self, TSTable)
end

function TSTable:getStat()
    return {
        dataFile = self.filePath,
        startTime = self.startTime,
        endTime = self.endTime,
        interval = self.config.schema[1].interval,
        fileSize = self.fileSize,
        recordSize = self.config.recordSize,
        estimatedRows = math.floor(self.fileSize / self.config.recordSize),
    }
end

function TSTable:queryRange(queryStart, queryEnd, filterZero)
    local recordSize = self.config.recordSize
    local maxRecordsInBatch = math.floor(8192 / recordSize)
    
    local interval = self.config.schema[1].interval
    queryStart = alignToInterval(queryStart, interval)
    queryEnd = alignToInterval(queryEnd, interval)
    local actualStart = math.max(queryStart, self.startTime)
    local actualEnd = math.min(queryEnd, self.endTime)
    if actualStart > actualEnd then return {} end

    local startIndex = math.floor((actualStart - self.startTime) / interval)
    local endIndex = math.floor((actualEnd - self.startTime) / interval)
    local numRecordsRemaining = endIndex - startIndex + 1

    local file = io.open(self.filePath, "rb")
    if not file then return {} end
    file:seek("set", startIndex * recordSize)

    local records = {}
    while numRecordsRemaining > 0 do
        local recordsToRead = math.min(numRecordsRemaining, maxRecordsInBatch)
        local batchSize = recordsToRead * recordSize
        local bulkBinary = file:read(batchSize)
        if not bulkBinary or #bulkBinary == 0 then
            break 
        end
        local currentOffset = 1
        local actualRecordsInBatch = math.floor(#bulkBinary / recordSize)
        for i = 1, actualRecordsInBatch do
            local binaryRecord = bulkBinary:sub(currentOffset, currentOffset + recordSize - 1)
            local record = TSPacker.unpackRecord(self.config, binaryRecord)
            if record[1] >= actualStart and record[1] <= actualEnd then
                if not filterZero or not TSPacker.isZeroRecord(record) then
                    table.insert(records, record)
                end
            end
            currentOffset = currentOffset + recordSize
        end
        numRecordsRemaining = numRecordsRemaining - actualRecordsInBatch
    end
    file:close()
    return records
end

function TSTable:queryRangeAgg(queryStart, queryEnd, aggInterval, aggs)
    local columnNames = self.config.columnNames
    local records = self:queryRange(queryStart, queryEnd, true)
    local result = {}
    local lastAggTime
    local aggRecord
    for _, record in ipairs(records) do
        local currentAggTime = alignToInterval(record[1], aggInterval)
        if currentAggTime ~= lastAggTime then
            aggRecord = { currentAggTime }
            table.insert(result, aggRecord)
            lastAggTime = currentAggTime
        end
        for j, aggItem in ipairs(aggs) do
            local id = columnNames[aggItem.columnName]
            aggRecord[j + 1] = aggItem.aggFunction(aggRecord[j + 1], record[id])
        end
    end
    return result
end

function TSTable:writeRecords(recordsArray)
    if self.readOnly or #recordsArray == 0 then
        return 0
    end

    local interval = self.config.schema[1].interval
    local lastRecordTime = self.endTime
    local firstWriteRecordTime = nil
    local packedBatch = {}

    for i, record in ipairs(recordsArray) do
        record[1] = alignToInterval(record[1], interval)
        local recordTime = record[1]
        if recordTime < lastRecordTime then
            goto continue
        else
            if recordTime > lastRecordTime + interval then
                local gapCount = math.floor((recordTime - lastRecordTime) / interval) - 1
                for i = 1, gapCount do
                    local zeroRecord = TSPacker.createZeroRecord(self.config, lastRecordTime + interval)
                    local packedZeroRecord = TSPacker.packRecordFromArray(self.config, zeroRecord)
                    table.insert(packedBatch, packedZeroRecord)
                    lastRecordTime = lastRecordTime + interval
                    if(not firstWriteRecordTime) then
                        firstWriteRecordTime = lastRecordTime
                    end
                end
            end
            local packedRecord = TSPacker.packRecordFromArray(self.config, record)
            table.insert(packedBatch, packedRecord)
            lastRecordTime = recordTime
            if not firstWriteRecordTime then
                firstWriteRecordTime = recordTime
            end
        end

        ::continue::
    end

    local batchSize = #packedBatch
    if batchSize > 0 then
        local file = io.open(self.filePath, "r+b")
        if not file then
            error(string.format("Failed to open data file for writing: %s", self.filePath))
        end
        if firstWriteRecordTime == self.endTime then
            file:seek("set", self.fileSize - self.config.recordSize)
        elseif firstWriteRecordTime > self.endTime then
            file:seek("set", self.fileSize)
        end
        file:write(table.concat(packedBatch))
        file:flush()
        file:close()
        self.endTime = lastRecordTime
    end
    return batchSize
end

return M