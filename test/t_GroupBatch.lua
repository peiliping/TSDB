local Group = require("record.GroupBatch")
local Record = require("record.Record") -- For type checking in mocks

local test_case = {}

-- Helper function for asserting errors
local function assertErrorMsgContains(expected_msg, func)
    local success, err = pcall(func)
    assert(not success, "Expected an error, but no error occurred.")
    assert(string.find(err, expected_msg), "Error message '" .. err .. "' does not contain '" .. expected_msg .. "'")
end

-- Mock Column object (reused from previous tests)
local MockColumn = {}
function MockColumn:new(name, format, size)
    local o = {
        name = name,
        format = format,
        size = size,
    }
    setmetatable(o, self)
    self.__index = self
    return o
end

-- Mock Columns object (reused from previous tests)
local MockColumns = {}
function MockColumns:new(cols_definition, interval)
    local o = {
        cols = cols_definition,
        name_to_index = {},
        nil_record_flags = 0,
        format_string = "I", -- Assuming nil_flags is an unsigned int
        interval = interval or 60, -- Default interval
    }
    setmetatable(o, self)
    self.__index = self

    for i, col in ipairs(cols_definition) do
        o.name_to_index[col.name] = i
        o.format_string = o.format_string .. col.format
        -- Calculate nil_record_flags: all bits set for nil record
        o.nil_record_flags = bit.bor(o.nil_record_flags, bit.lshift(1, i - 1)) -- Using bit.bor and bit.lshift for clarity
    end
    return o
end

function MockColumns:count()
    return #self.cols
end

function MockColumns:get_index_by_name(name)
    local index = self.name_to_index[name]
    if not index then
        error("Column not found: " .. name)
    end
    return index
end

function MockColumns:get_interval()
    return self.interval
end

-- Mock Record object
local MockRecord = {}
function MockRecord:new(columns, data, nil_flags)
    local o = {
        columns = columns,
        data = data,
        nil_flags = nil_flags,
    }
    setmetatable(o, self)
    self.__index = self
    return o
end

function MockRecord:getTimestamp()
    return self.data[1]
end

function MockRecord:get_value(column_name)
    local index = self.columns:get_index_by_name(column_name)
    if bit.band(self.nil_flags, bit.lshift(1, index - 1)) ~= 0 then
        return nil
    end
    return self.data[index]
end

function MockRecord:is_nil_record()
    return self.nil_flags == self.columns.nil_record_flags
end

-- Mock Batch object
local MockBatch = {}
function MockBatch:new(columns, records_data)
    local self = {
        columns = columns,
        records = {},
    }
    setmetatable(self, MockBatch)
    self.__index = self

    for _, rec_data in ipairs(records_data) do
        table.insert(self.records, MockRecord:new(columns, rec_data.data, rec_data.nil_flags))
    end
    return self
end

function MockBatch:count()
    return #self.records
end

function MockBatch:get_record(index)
    return self.records[index]
end

-- Mock DataTable object
local MockDataTable = {}
function MockDataTable:new(columns, interval, records_map)
    local self = {
        columns = columns,
        interval = interval,
        records_map = records_map or {}, -- Map: start_time -> {end_time, records_data_for_batch}
    }
    setmetatable(self, MockDataTable)
    self.__index = self
    return self
end

function MockDataTable:query_records(start_time, end_time, filter_nil)
    local batch_data = self.records_map[start_time]
    if batch_data and batch_data.end_time == end_time then
        -- Simulate filtering nil records if requested
        local filtered_records_data = {}
        for _, rec_data in ipairs(batch_data.records) do
            local is_nil = (rec_data.nil_flags == self.columns.nil_record_flags)
            if not (filter_nil and is_nil) then
                table.insert(filtered_records_data, rec_data)
            end
        end
        return MockBatch:new(self.columns, filtered_records_data)
    end
    return MockBatch:new(self.columns, {}) -- Return empty batch if no match
end

-- Test for Group.new
function test_case.test_Group_new()
    local col1 = MockColumn:new("timestamp", "I4", 4)
    local mock_cols = MockColumns:new({ col1 }, 60)
    local mock_data_table = MockDataTable:new(mock_cols, 60)

    local start_time = 1000
    local end_time = 2000
    local records_per_batch = 100
    local filter_nil = true

    local group = Group.new(mock_data_table, start_time, end_time, records_per_batch, filter_nil)

    assert(group ~= nil, "Group should be created")
    assert(group.data_table == mock_data_table, "data_table should match")
    assert(group.interval == mock_data_table.interval, "interval should match")
    assert(group.total_start == start_time, "total_start should match")
    assert(group.total_end == end_time, "total_end should match")
    assert(group.records_per_batch == records_per_batch, "records_per_batch should match")
    assert(group.chunk_duration == (records_per_batch - 1) * mock_data_table.interval, "chunk_duration should be calculated correctly")
    assert(group.filter_nil == filter_nil, "filter_nil should match")
    assert(group.current_start == start_time, "current_start should initialize to total_start")
    assert(group.current_index == 0, "current_index should initialize to 0")

    -- Test default records_per_batch and filter_nil
    local group_defaults = Group.new(mock_data_table, start_time, end_time)
    assert(group_defaults.records_per_batch == 100000, "records_per_batch should default to 100000")
    assert(group_defaults.filter_nil == false, "filter_nil should default to false")
end

-- Test for Group:next basic iteration
function test_case.test_Group_next_basic()
    local col1 = MockColumn:new("timestamp", "I4", 4)
    local col2 = MockColumn:new("value", "f", 4)
    local mock_cols = MockColumns:new({ col1, col2 }, 60)
    local ts_base = 1678886400 -- A fixed timestamp

    -- Simulate data for query_records
    local records_map = {
        [ts_base] = {
            end_time = ts_base + 60 * 2, -- 3 records
            records = {
                { data = { ts_base, 10.0 }, nil_flags = 0 },
                { data = { ts_base + 60, 20.0 }, nil_flags = 0 },
                { data = { ts_base + 120, 30.0 }, nil_flags = 0 },
            }
        }
    }
    local mock_data_table = MockDataTable:new(mock_cols, 60, records_map)

    local group = Group.new(mock_data_table, ts_base, ts_base + 120, 100) -- records_per_batch > total records

    local rec1 = group:next()
    assert(rec1 ~= nil, "First record should not be nil")
    assert(rec1:getTimestamp() == ts_base, "First record timestamp incorrect")
    assert(rec1:get_value("value") == 10.0, "First record value incorrect")

    local rec2 = group:next()
    assert(rec2 ~= nil, "Second record should not be nil")
    assert(rec2:getTimestamp() == ts_base + 60, "Second record timestamp incorrect")
    assert(rec2:get_value("value") == 20.0, "Second record value incorrect")

    local rec3 = group:next()
    assert(rec3 ~= nil, "Third record should not be nil")
    assert(rec3:getTimestamp() == ts_base + 120, "Third record timestamp incorrect")
    assert(rec3:get_value("value") == 30.0, "Third record value incorrect")

    local rec4 = group:next()
    assert(rec4 == nil, "Should be no more records")
end

-- Test for Group:next across multiple batches
function test_case.test_Group_next_multiple_batches()
    local col1 = MockColumn:new("timestamp", "I4", 4)
    local mock_cols = MockColumns:new({ col1 }, 60)
    local ts_base = 1678886400

    -- Simulate data for query_records, two batches
    local records_map = {
        [ts_base] = {
            end_time = ts_base + 60, -- 2 records
            records = {
                { data = { ts_base, 100 }, nil_flags = 0 },
                { data = { ts_base + 60, 110 }, nil_flags = 0 },
            }
        },
        [ts_base + 120] = { -- Note the gap, query_records will be called for ts_base + 120
            end_time = ts_base + 180, -- 2 records
            records = {
                { data = { ts_base + 120, 120 }, nil_flags = 0 },
                { data = { ts_base + 180, 130 }, nil_flags = 0 },
            }
        }
    }
    local mock_data_table = MockDataTable:new(mock_cols, 60, records_map)

    -- records_per_batch = 2, so each query_records call should fetch 2 records
    local group = Group.new(mock_data_table, ts_base, ts_base + 180, 2)

    -- First batch
    assert(group:next():getTimestamp() == ts_base)
    assert(group:next():getTimestamp() == ts_base + 60)

    -- Transition to next batch (current_start will be ts_base + 120)
    assert(group:next():getTimestamp() == ts_base + 120)
    assert(group:next():getTimestamp() == ts_base + 180)

    assert(group:next() == nil, "Should be no more records")
end

-- Test for Group:next with filter_nil
function test_case.test_Group_next_with_filter_nil()
    local col1 = MockColumn:new("timestamp", "I4", 4)
    local col2 = MockColumn:new("value", "f", 4)
    local mock_cols = MockColumns:new({ col1, col2 }, 60)
    local ts_base = 1678886400

    local nil_flags_for_value = bit.lshift(1, 1) -- col2 (value) is nil
    local all_nil_flags = mock_cols.nil_record_flags

    -- Simulate data for query_records
    local records_map = {
        [ts_base] = {
            end_time = ts_base + 120,
            records = {
                { data = { ts_base, 10.0 }, nil_flags = 0 },
                { data = { ts_base + 60, nil }, nil_flags = nil_flags_for_value }, -- Record with nil value
                { data = { ts_base + 120, nil }, nil_flags = all_nil_flags }, -- Fully nil record
            }
        }
    }
    local mock_data_table = MockDataTable:new(mock_cols, 60, records_map)

    -- Test without filter_nil (default)
    local group_no_filter = Group.new(mock_data_table, ts_base, ts_base + 120, 100)
    assert(group_no_filter:next():getTimestamp() == ts_base)
    assert(group_no_filter:next():getTimestamp() == ts_base + 60)
    assert(group_no_filter:next():getTimestamp() == ts_base + 120)
    assert(group_no_filter:next() == nil)

    -- Test with filter_nil = true
    local group_with_filter = Group.new(mock_data_table, ts_base, ts_base + 120, 100, true)
    local rec1 = group_with_filter:next()
    assert(rec1 ~= nil)
    assert(rec1:getTimestamp() == ts_base)
    assert(rec1:get_value("value") == 10.0)

    local rec2 = group_with_filter:next()
    assert(rec2 ~= nil)
    assert(rec2:getTimestamp() == ts_base + 60)
    assert(rec2:get_value("value") == nil) -- This record is not fully nil, so it should pass filter

    local rec3 = group_with_filter:next()
    assert(rec3 == nil, "Fully nil record should be filtered out")
end

-- Test for Group:next with empty range
function test_case.test_Group_next_empty_range()
    local col1 = MockColumn:new("timestamp", "I4", 4)
    local mock_cols = MockColumns:new({ col1 }, 60)
    local mock_data_table = MockDataTable:new(mock_cols, 60)

    local start_time = 2000
    local end_time = 1000 -- start > end

    local group = Group.new(mock_data_table, start_time, end_time)
    assert(group:next() == nil, "Should return nil immediately for empty range")
end

-- Test for Group:iterator
function test_case.test_Group_iterator()
    local col1 = MockColumn:new("timestamp", "I4", 4)
    local mock_cols = MockColumns:new({ col1 }, 60)
    local ts_base = 1678886400

    local records_map = {
        [ts_base] = {
            end_time = ts_base + 60,
            records = {
                { data = { ts_base, 100 }, nil_flags = 0 },
                { data = { ts_base + 60, 110 }, nil_flags = 0 },
            }
        }
    }
    local mock_data_table = MockDataTable:new(mock_cols, 60, records_map)

    local group = Group.new(mock_data_table, ts_base, ts_base + 60, 100)
    local iter = group:iterator()

    local count = 0
    for record in iter do
        count = count + 1
        assert(record ~= nil, "Record from iterator should not be nil")
        if count == 1 then assert(record:getTimestamp() == ts_base) end
        if count == 2 then assert(record:getTimestamp() == ts_base + 60) end
    end
    assert(count == 2, "Iterator should have returned 2 records")
end

return test_case