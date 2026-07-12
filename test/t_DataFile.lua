local TestTools = require("test.TestTools")

local BinaryTools = require("tools.BinaryTools")

local DataFile = require("db.DataFile")
local Headers = require("db.Headers")

local Columns = require("record.col.Columns")
local TimeCol = require("record.col.TimeCol")
local NumberCol = require("record.col.NumberCol")
local Batch = require("record.Batch")

local test_case = {}

local TEST_DIR = "./test_data/"
local TEST_FILE = TEST_DIR .. "test_datafile.bin"
local TEST_BLOCK_SIZE = 1024
local TEST_INTERVAL = 60

local col_timestamp = TimeCol.new("timestamp", TEST_INTERVAL)
local col_value = NumberCol.new("value", "number", 0, false)
local real_cols = Columns.new({ col_timestamp, col_value })

local TEST_RECORD_SIZE = real_cols.record_size

local function create_real_batch(start_ts, count)
    local current_batch = Batch.new(real_cols)
    for i = 0, count - 1 do
        local ts = start_ts + i * TEST_INTERVAL
        local value = math.random(1, 100)
        current_batch:add({ ts, value })
    end
    return current_batch
end

local function setup()
    os.execute("mkdir -p " .. TEST_DIR)
    os.execute("rm -f " .. TEST_FILE)
end

local function teardown()
    os.execute("rm -rf " .. TEST_DIR)
end

function test_case.test_new_and_exist()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    assert(not df:exist(), "File should not exist initially")
    teardown()
end

function test_case.test_create_and_load()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()
    assert(df:exist(), "File should exist after creation")

    local f = io.open(TEST_FILE, "rb")
    local content = f:read("a")
    f:close()
    assert(Headers.header_length + TEST_BLOCK_SIZE == #content, "Initial file size mismatch")

    local loaded_df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    loaded_df:load()
    assert(0 == loaded_df.start_time, "Initial start_time should be 0")
    assert(0 == loaded_df.end_time, "Initial end_time should be 0")
    assert(Headers.header_length + TEST_BLOCK_SIZE == loaded_df.file_size, "Loaded file size mismatch")
    teardown()
end

function test_case.test_write_single_batch_to_empty_file()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts = 1000 * TEST_INTERVAL
    local count = 3
    local batch = create_real_batch(start_ts, count)

    local written_count = df:write(batch)
    assert(count == written_count, "Written count mismatch")
    assert(batch:start_time() == df.start_time, "start_time after first write mismatch")
    assert(batch:end_time() == df.end_time, "end_time after first write mismatch")

    local f = io.open(TEST_FILE, "rb")
    local header_binary = f:read(Headers.header_length)
    local file_start_time, file_end_time = BinaryTools.unpack_header(header_binary, TEST_INTERVAL, TEST_RECORD_SIZE)
    assert(batch:start_time() == file_start_time, "Header start_time mismatch")
    assert(batch:end_time() == file_end_time, "Header end_time mismatch")

    f:seek("set", Headers.header_length)
    local data_binary = f:read(count * TEST_RECORD_SIZE)
    assert(batch:to_binary() == data_binary, "Written data mismatch")
    f:close()
    teardown()
end

function test_case.test_write_multiple_batches_append()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts1 = 1000 * TEST_INTERVAL
    local count1 = 3
    local batch1 = create_real_batch(start_ts1, count1)
    df:write(batch1)

    local start_ts2 = batch1:end_time() + TEST_INTERVAL
    local count2 = 2
    local batch2 = create_real_batch(start_ts2, count2)
    df:write(batch2)

    assert(batch1:start_time() == df.start_time, "start_time after append mismatch")
    assert(batch2:end_time() == df.end_time, "end_time after append mismatch")
    assert(batch1:count() + batch2:count() == df:count(), "Total count after append mismatch")

    teardown()
end

function test_case.test_write_multiple_batches_overwrite()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts1 = 1000 * TEST_INTERVAL
    local count1 = 5
    local batch1 = create_real_batch(start_ts1, count1)
    df:write(batch1)

    local start_ts2 = start_ts1 + TEST_INTERVAL * 1
    local count2 = 2
    local batch2 = create_real_batch(start_ts2, count2)
    df:write(batch2)

    assert(batch1:start_time() == df.start_time, "start_time after overwrite mismatch")
    assert(batch1:end_time() == df.end_time, "end_time after overwrite mismatch")
    assert(batch1:count() == df:count(), "Total count after overwrite mismatch")

    local f = io.open(TEST_FILE, "rb")
    f:seek("set", Headers.header_length + (start_ts2 - start_ts1) / TEST_INTERVAL * TEST_RECORD_SIZE)
    local overwritten_data = f:read(count2 * TEST_RECORD_SIZE)
    assert(batch2:to_binary() == overwritten_data, "Overwritten data mismatch")
    f:close()

    teardown()
end

function test_case.test_write_file_expansion()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local initial_file_size = Headers.header_length + TEST_BLOCK_SIZE
    assert(initial_file_size == df.file_size, "Initial file size incorrect")

    local start_ts = 1000 * TEST_INTERVAL
    local count = math.floor((TEST_BLOCK_SIZE * 2) / TEST_RECORD_SIZE) + 1
    local batch = create_real_batch(start_ts, count)

    df:write(batch)

    local expected_min_size = Headers.header_length + count * TEST_RECORD_SIZE
    local expected_file_size = math.ceil((expected_min_size - Headers.header_length) / TEST_BLOCK_SIZE) * TEST_BLOCK_SIZE + Headers.header_length
    assert(df.file_size >= expected_min_size, "File size should have expanded")
    assert(expected_file_size == df.file_size, "File size should be a multiple of block size")

    teardown()
end

function test_case.test_write_error_out_of_range_start_time()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts1 = 1000 * TEST_INTERVAL
    local count1 = 3
    local batch1 = create_real_batch(start_ts1, count1)
    df:write(batch1)

    local start_ts_error = (1000 - 1) * TEST_INTERVAL
    local count_error = 2
    local batch_error = create_real_batch(start_ts_error, count_error)

    TestTools.assert_error_msg_contains("Out of range", function() df:write(batch_error) end)

    teardown()
end

function test_case.test_write_error_data_gap()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts1 = 1000 * TEST_INTERVAL
    local count1 = 3
    local batch1 = create_real_batch(start_ts1, count1)
    df:write(batch1)

    local start_ts_gap = batch1:end_time() + TEST_INTERVAL * 2
    local count_gap = 2
    local batch_gap = create_real_batch(start_ts_gap, count_gap)

    TestTools.assert_error_msg_contains("Data Gap detected", function() df:write(batch_gap) end)

    teardown()
end

local function compare_batches(expected_batch, actual_batch, test_name)
    assert(expected_batch:count() == actual_batch:count(), test_name .. ": Batch count mismatch")
    assert(expected_batch:start_time() == actual_batch:start_time(), test_name .. ": Batch start_time mismatch")
    assert(expected_batch:end_time() == actual_batch:end_time(), test_name .. ": Batch end_time mismatch")

    for i = 1, expected_batch:count() do
        local expected_record = expected_batch:get_record(i)
        local actual_record = actual_batch:get_record(i)

        assert(expected_record:get_timestamp() == actual_record:get_timestamp(), test_name .. ": Record " .. i .. " timestamp mismatch")
        assert(expected_record.nil_flags == actual_record.nil_flags, test_name .. ": Record " .. i .. " nil_flags mismatch")

        local expected_value = expected_record:get_value("value")
        local actual_value = actual_record:get_value("value")

        if expected_value == nil and actual_value == nil then
            assert(true, test_name .. ": Record " .. i .. " value: both nil")
        elseif expected_value ~= nil and actual_value ~= nil then
            assert(math.abs(expected_value - actual_value) < 0.001, test_name .. ": Record " .. i .. " value mismatch (non-nil)")
        else
            assert(false, test_name .. ": Record " .. i .. " value mismatch (one nil, one not)")
        end
    end
end

function test_case.test_read_full_range()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts = 1000 * TEST_INTERVAL
    local count = 5
    local batch_to_write = create_real_batch(start_ts, count)
    df:write(batch_to_write)

    local read_batch_obj = Batch.new(real_cols)
    df:read(read_batch_obj, batch_to_write:start_time(), batch_to_write:end_time())
    compare_batches(batch_to_write, read_batch_obj, "test_read_full_range")

    teardown()
end

function test_case.test_read_partial_range()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts = 1000 * TEST_INTERVAL
    local count = 10
    local full_batch_to_write = create_real_batch(start_ts, count)
    df:write(full_batch_to_write)

    local read_start_ts = start_ts + TEST_INTERVAL * 2
    local read_end_ts = start_ts + TEST_INTERVAL * 5

    local read_batch_obj = Batch.new(real_cols)
    df:read(read_batch_obj, read_start_ts, read_end_ts)

    local expected_partial_batch = Batch.new(real_cols)
    for ts = read_start_ts, read_end_ts, TEST_INTERVAL do
        local record_index = (ts - full_batch_to_write:start_time()) / TEST_INTERVAL + 1
        local record = full_batch_to_write:get_record(record_index)
        expected_partial_batch:add_record(record)
    end
    compare_batches(expected_partial_batch, read_batch_obj, "test_read_partial_range")

    teardown()
end

function test_case.test_read_beyond_file_boundaries()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts = 1000 * TEST_INTERVAL
    local count = 5
    local batch_to_write = create_real_batch(start_ts, count)
    df:write(batch_to_write)

    local read_batch_obj = Batch.new(real_cols)

    df:read(read_batch_obj, start_ts - TEST_INTERVAL, batch_to_write:end_time() + TEST_INTERVAL)
    compare_batches(batch_to_write, read_batch_obj, "test_read_beyond_file_boundaries_overlap")

    teardown()
end

function test_case.test_count()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()
    assert(0 == df:count(), "Count should be 0 for empty file")

    local start_ts = 1000 * TEST_INTERVAL
    local count = 5
    local batch_to_write = create_real_batch(start_ts, count)
    df:write(batch_to_write)
    assert(batch_to_write:count() == df:count(), "Count mismatch after writing data")

    teardown()
end

return test_case
