local M = {}

local TSPacker = require("TSPacker")
local RingBuffer = require("RingBuffer")

local TSTable = {}
TSTable.__index = TSTable

local function getFileSize(filePath)
    local file, err = io.open(filePath, "rb")
    if err then error("Read File Error : " .. err) end
    if not file then error(filePath .. " is nil") end
    local size = file:seek("end")
    file:close() 
    return size or 0
end

local function getStartTime(filePath, schema)
    local file = io.open(filePath, "rb")
    if not file then error("Failed to open data file for getting startTime.") end
    file:seek("set", 0)
    local firstRecordBinary = file:read(schema.recordSize)
    file:close()
    local record = TSPacker.unpackRecord(schema, firstRecordBinary)
    return record[1]
end

local function alignToInterval(ts, interval)
    return math.floor(ts / interval) * interval
end

function M.new(schema, filePath, readOnly)
    
    TSPacker.calculateSchema(schema)
    local self = {
        schema = schema,
        filePath = filePath,
        fileSize = 0,
        startTime = 0,
        endTime = 0,
        interval = schema.columns[1].interval,
        readOnly = readOnly,
    }

     ::retry::

    local recordSize = self.schema.recordSize
    self.fileSize = getFileSize(self.filePath)
    if self.fileSize >= recordSize then
        self.startTime = getStartTime(self.filePath, self.schema)
    end
    local numFullRecords = math.floor(self.fileSize / recordSize)

    if not readOnly then
        if self.fileSize == 0 then
            -- nothing
        elseif self.fileSize > 0 and self.fileSize < recordSize then
            error("Invalid Data File : " .. self.filePath)
        else
            if self.fileSize % recordSize > 0 then
                local file = io.open(self.filePath, "r+b")
                if not file then error(string.format("Failed to open data file for fixing : %s", self.filePath)) end
                file:seek("set", numFullRecords * recordSize)
                local nextRecordTime = self.startTime + numFullRecords * self.interval
                file:write(TSPacker.createZeroRecordBin(self.schema, nextRecordTime))
                file:flush()
                file:close()
                goto retry
            end
        end
    end
    if numFullRecords > 0 then
        self.endTime = self.startTime + (numFullRecords - 1) * self.interval
    end
    return setmetatable(self, TSTable)
end

function TSTable:getStat()
    return {
        dataFile = self.filePath,
        startTime = self.startTime,
        endTime = self.endTime,
        interval = self.interval,
        fileSize = self.fileSize,
        recordSize = self.schema.recordSize,
        estimatedRows = math.floor(self.fileSize / self.schema.recordSize),
    }
end

function TSTable:queryRange(queryStart, queryEnd, filterZero)
    if self.fileSize == 0 then return {} end
    local recordSize = self.schema.recordSize
    local maxRecordsInBatch = math.floor(1048576 / recordSize)

    queryStart = alignToInterval(queryStart, self.interval)
    queryEnd = alignToInterval(queryEnd, self.interval)
    local actualStart = math.max(queryStart, self.startTime)
    local actualEnd = math.min(queryEnd, self.endTime)
    if actualStart > actualEnd then return {} end

    local startIndex = math.floor((actualStart - self.startTime) / self.interval)
    local endIndex = math.floor((actualEnd - self.startTime) / self.interval)
    local numRecordsRemaining = endIndex - startIndex + 1

    local zeroRecordDataBin = TSPacker.getZeroRecordBinDataPart(self.schema)

    local file = io.open(self.filePath, "rb")
    if not file then return {} end
    file:seek("set", startIndex * recordSize)

    local records = {}
    local count = 0
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
            if not filterZero or not TSPacker.isZeroRecord(binaryRecord, zeroRecordDataBin) then
                local record = TSPacker.unpackRecord(self.schema, binaryRecord)
                count = count + 1
                records[count] = record
            end
            currentOffset = currentOffset + recordSize
        end
        numRecordsRemaining = numRecordsRemaining - actualRecordsInBatch
    end
    file:close()
    return records
end

function TSTable:queryAggTumbling(queryStart, queryEnd, aggInterval, aggs)
    if self.fileSize == 0 then return {} end
    local recordSize = self.schema.recordSize
    local maxRecordsInBatch = math.floor(1048576 / recordSize)

    queryStart = alignToInterval(queryStart, self.interval)
    queryEnd = alignToInterval(queryEnd, self.interval)
    local actualStart = math.max(queryStart, self.startTime)
    local actualEnd = math.min(queryEnd, self.endTime)
    if actualStart > actualEnd then return {} end

    local startIndex = math.floor((actualStart - self.startTime) / self.interval)
    local endIndex = math.floor((actualEnd - self.startTime) / self.interval)
    local numRecordsRemaining = endIndex - startIndex + 1

    local zeroRecordDataBin = TSPacker.getZeroRecordBinDataPart(self.schema)

    local file = io.open(self.filePath, "rb")
    if not file then return {} end
    file:seek("set", startIndex * recordSize)

    local records = {}
    local count = 0
    local aggRecord
    local lastAggTime
    local currentAggTime

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
            if not TSPacker.isZeroRecord(binaryRecord, zeroRecordDataBin) then
                local record = TSPacker.unpackRecord(self.schema, binaryRecord)
                currentAggTime = alignToInterval(record[1], aggInterval)
                if currentAggTime ~= lastAggTime then
                    count = count + 1
                    aggRecord = { currentAggTime }
                    records[count] = aggRecord
                    lastAggTime = currentAggTime
                end
                for j, aggItem in ipairs(aggs) do
                    aggRecord[j + 1] = aggItem.aggFunction(aggRecord[j + 1], record[aggItem.columnId])
                end
            end
            currentOffset = currentOffset + recordSize
        end
        numRecordsRemaining = numRecordsRemaining - actualRecordsInBatch
    end
    file:close()
    return records
end

function TSTable:queryAggSliding(queryStart, queryEnd, aggInterval, aggs)
    if self.fileSize == 0 then return {} end
    local recordSize = self.schema.recordSize
    local maxRecordsInBatch = math.floor(1048576 / recordSize)

    queryStart = alignToInterval(queryStart, self.interval)
    queryEnd = alignToInterval(queryEnd, self.interval)
    local actualStart = math.max(queryStart, self.startTime)
    local actualEnd = math.min(queryEnd, self.endTime)
    if actualStart > actualEnd then return {} end

    local startIndex = math.floor((actualStart - self.startTime) / self.interval)
    local endIndex = math.floor((actualEnd - self.startTime) / self.interval)
    local numRecordsRemaining = endIndex - startIndex + 1

    local zeroRecordDataBin = TSPacker.getZeroRecordBinDataPart(self.schema)

    local file = io.open(self.filePath, "rb")
    if not file then return {} end
    file:seek("set", startIndex * recordSize)

    local records = {}
    local count = 0
    local aggRecord
    local columnArray = {}
    local ringbuffer = RingBuffer.new(aggInterval)
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
            if not filterZero or not TSPacker.isZeroRecord(binaryRecord, zeroRecordDataBin) then
                local record = TSPacker.unpackRecord(self.schema, binaryRecord)
                ringbuffer:add(record)
                if ringbuffer:isFull() then
                    count = count + 1
                    aggRecord = { record[1] }
                    records[count] = aggRecord
                    for j, aggItem in ipairs(aggs) do
                        for g = 1, ringbuffer:size() do
                            columnArray[g] = ringbuffer:get(g)[aggItem.columnId]
                        end
                        aggRecord[j + 1] = aggItem.aggFunction(columnArray)
                    end
                end
            end
            currentOffset = currentOffset + recordSize
        end
        numRecordsRemaining = numRecordsRemaining - actualRecordsInBatch
    end
    file:close()
    return records
end

function TSTable:writeRecords(recordsArray)
    if self.readOnly or #recordsArray == 0 then return 0 end

    local lastRecordTime = self.endTime
    local firstWriteRecordTime = nil
    local packedBatch = {}
    local packedBatchSize = 0

    for i, record in ipairs(recordsArray) do
        local recordTime = alignToInterval(record[1], self.interval)
        if recordTime < lastRecordTime then
            -- 历史数据
            goto continue
        elseif recordTime == lastRecordTime then
            -- 更新的情况 保留最后一条
            if not firstWriteRecordTime then
                firstWriteRecordTime = recordTime
                packedBatchSize = packedBatchSize + 1
            end
            packedBatch[packedBatchSize] = TSPacker.packRecord(self.schema, record)
        else 
            -- 补gap
            if lastRecordTime > 0 and recordTime > lastRecordTime + self.interval then
                local gapCount = math.floor((recordTime - lastRecordTime) / self.interval) - 1
                for i = 1, gapCount do
                    packedBatchSize = packedBatchSize + 1
                    lastRecordTime = lastRecordTime + self.interval
                    packedBatch[packedBatchSize] = TSPacker.createZeroRecordBin(self.schema, lastRecordTime)
                    if(not firstWriteRecordTime) then
                        firstWriteRecordTime = lastRecordTime
                    end
                end
            end
            -- 正常
            packedBatchSize = packedBatchSize + 1
            packedBatch[packedBatchSize] = TSPacker.packRecord(self.schema, record)
            lastRecordTime = recordTime
            if not firstWriteRecordTime then
                firstWriteRecordTime = recordTime
            end
        end

        ::continue::
    end

    if packedBatchSize > 0 then
        local file = io.open(self.filePath, "r+b")
        if not file then error(string.format("Failed to open data file for writing: %s", self.filePath)) end
        local fileSizeIncr = 0
        if firstWriteRecordTime == self.endTime then
            file:seek("set", self.fileSize - self.schema.recordSize)
            fileSizeIncr = (packedBatchSize - 1) * self.schema.recordSize
        elseif firstWriteRecordTime > self.endTime then
            file:seek("set", self.fileSize)
            fileSizeIncr = packedBatchSize * self.schema.recordSize
        end
        file:write(table.concat(packedBatch))
        file:flush()
        file:close()
        if self.startTime == 0 then
            self.startTime = firstWriteRecordTime
        end
        self.endTime = lastRecordTime
        self.fileSize = self.fileSize + fileSizeIncr
    end
    return packedBatchSize
end

return M