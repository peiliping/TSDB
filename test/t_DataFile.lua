local TestTools = require("test.TestTools")
local DataFile = require("db.DataFile")
local Headers = require("db.Headers")
local BinaryTools = require("tools.BinaryTools")

local t_DataFile = {}

local TEST_DIR = "./test_data/"
local TEST_FILE = TEST_DIR .. "test_datafile.bin"
local TEST_BLOCK_SIZE = 1024
local TEST_INTERVAL = 60 -- 1 minute interval
local TEST_RECORD_SIZE = 8 -- Dummy record size for testing

-- Helper function to create a dummy Batch object
local function create_dummy_batch(start_time, end_time, count, record_size)
    local batch = {}
    batch.start_ts = start_time
    batch.end_ts = end_time
    batch.record_count = count
    batch.record_size = record_size

    function batch:count()
        return self.record_count
    end

    function batch:start_time()
        return self.start_ts
    end

    function batch:end_time()
        return self.end_ts
    end

    function batch:toBinary()
        -- Simulate binary data for the batch
        return string.rep("A", self.record_count * self.record_size)
    end
    return batch
end

-- Setup and Teardown
local function setup()
    os.execute("mkdir -p " .. TEST_DIR)
    os.execute("rm -f " .. TEST_FILE)
end

local function teardown()
    os.execute("rm -rf " .. TEST_DIR)
end

function t_DataFile.test_new_and_exist()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    assert(not df:exist(), "File should not exist initially")
    teardown()
end

function t_DataFile.test_create_and_load()
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

function t_DataFile.test_write_single_batch_to_empty_file()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts = 1000
    local end_ts = 1000 + TEST_INTERVAL * 2
    local count = 3
    local batch = create_dummy_batch(start_ts, end_ts, count, TEST_RECORD_SIZE)

    local written_count = df:write(batch)
    assert(count == written_count, "Written count mismatch")
    assert(start_ts == df.start_time, "start_time after first write mismatch")
    assert(end_ts == df.end_time, "end_time after first write mismatch")

    -- Verify file content and header
    local f = io.open(TEST_FILE, "rb")
    local header_binary = f:read(Headers.header_length)
    local file_start_time, file_end_time = BinaryTools.unpack_header(header_binary, TEST_INTERVAL, TEST_RECORD_SIZE)
    assert(start_ts == file_start_time, "Header start_time mismatch")
    assert(end_ts == file_end_time, "Header end_time mismatch")

    f:seek("set", Headers.header_length)
    local data_binary = f:read(count * TEST_RECORD_SIZE)
    assert(batch:toBinary() == data_binary, "Written data mismatch")
    f:close()
    teardown()
end

function t_DataFile.test_write_multiple_batches_append()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts1 = 1000
    local end_ts1 = 1000 + TEST_INTERVAL * 2
    local count1 = 3
    local batch1 = create_dummy_batch(start_ts1, end_ts1, count1, TEST_RECORD_SIZE)
    df:write(batch1)

    local start_ts2 = end_ts1 + TEST_INTERVAL
    local end_ts2 = start_ts2 + TEST_INTERVAL * 1
    local count2 = 2
    local batch2 = create_dummy_batch(start_ts2, end_ts2, count2, TEST_RECORD_SIZE)
    df:write(batch2)

    assert(start_ts1 == df.start_time, "start_time after append mismatch")
    assert(end_ts2 == df.end_time, "end_time after append mismatch")
    assert(count1 + count2 == df:count(), "Total count after append mismatch")

    teardown()
end

function t_DataFile.test_write_multiple_batches_overwrite()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts1 = 1000
    local end_ts1 = 1000 + TEST_INTERVAL * 4
    local count1 = 5
    local batch1 = create_dummy_batch(start_ts1, end_ts1, count1, TEST_RECORD_SIZE)
    df:write(batch1)

    local start_ts2 = 1000 + TEST_INTERVAL * 1 -- Overlap with batch1
    local end_ts2 = start_ts2 + TEST_INTERVAL * 1
    local count2 = 2
    local batch2 = create_dummy_batch(start_ts2, end_ts2, count2, TEST_RECORD_SIZE)
    df:write(batch2)

    assert(start_ts1 == df.start_time, "start_time after overwrite mismatch")
    assert(end_ts1 == df.end_time, "end_time after overwrite mismatch")
    assert(count1 == df:count(), "Total count after overwrite mismatch") -- Count should remain based on original range

    -- Verify content of the overwritten part
    local f = io.open(TEST_FILE, "rb")
    f:seek("set", Headers.header_length + (start_ts2 - start_ts1) / TEST_INTERVAL * TEST_RECORD_SIZE)
    local overwritten_data = f:read(count2 * TEST_RECORD_SIZE)
    assert(batch2:toBinary() == overwritten_data, "Overwritten data mismatch")
    f:close()

    teardown()
end

function t_DataFile.test_write_file_expansion()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local initial_file_size = Headers.header_length + TEST_BLOCK_SIZE
    assert(initial_file_size == df.file_size, "Initial file size incorrect")

    -- Write a batch that exceeds the initial block size
    local start_ts = 1000
    local count = math.floor((TEST_BLOCK_SIZE * 2) / TEST_RECORD_SIZE) + 1 -- Ensure it spans multiple blocks
    local end_ts = start_ts + (count - 1) * TEST_INTERVAL
    local batch = create_dummy_batch(start_ts, end_ts, count, TEST_RECORD_SIZE)

    df:write(batch)

    local expected_min_size = Headers.header_length + count * TEST_RECORD_SIZE
    local expected_file_size = math.ceil((expected_min_size - Headers.header_length) / TEST_BLOCK_SIZE) * TEST_BLOCK_SIZE + Headers.header_length
    assert(df.file_size >= expected_min_size, "File size should have expanded")
    assert(expected_file_size == df.file_size, "File size should be a multiple of block size")

    teardown()
end

function t_DataFile.test_write_error_out_of_range_start_time()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts1 = 1000
    local end_ts1 = 1000 + TEST_INTERVAL * 2
    local batch1 = create_dummy_batch(start_ts1, end_ts1, 3, TEST_RECORD_SIZE)
    df:write(batch1)

    local start_ts_error = 900 -- Before file start_time
    local end_ts_error = start_ts_error + TEST_INTERVAL
    local batch_error = create_dummy_batch(start_ts_error, end_ts_error, 2, TEST_RECORD_SIZE)

    TestTools.assertErrorMsgContains("Out of range", function() df:write(batch_error) end)

    teardown()
end

function t_DataFile.test_write_error_data_gap()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts1 = 1000
    local end_ts1 = 1000 + TEST_INTERVAL * 2
    local batch1 = create_dummy_batch(start_ts1, end_ts1, 3, TEST_RECORD_SIZE)
    df:write(batch1)

    local start_ts_gap = end_ts1 + TEST_INTERVAL * 2 -- Gap of one interval
    local end_ts_gap = start_ts_gap + TEST_INTERVAL
    local batch_gap = create_dummy_batch(start_ts_gap, end_ts_gap, 2, TEST_RECORD_SIZE)

    TestTools.assertErrorMsgContains("Data Gap detected", function() df:write(batch_gap) end)

    teardown()
end

function t_DataFile.test_read_full_range()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts = 1000
    local end_ts = 1000 + TEST_INTERVAL * 4
    local count = 5
    local batch_to_write = create_dummy_batch(start_ts, end_ts, count, TEST_RECORD_SIZE)
    df:write(batch_to_write)

    local read_batch = {}
    function read_batch:fromBinary(binary_data)
        self.binary_data = binary_data
    end
    df:read(read_batch, start_ts, end_ts)
    assert(batch_to_write:toBinary() == read_batch.binary_data, "Read data mismatch for full range")

    teardown()
end

function t_DataFile.test_read_partial_range()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts = 1000
    local end_ts = 1000 + TEST_INTERVAL * 9
    local count = 10
    local batch_to_write = create_dummy_batch(start_ts, end_ts, count, TEST_RECORD_SIZE)
    df:write(batch_to_write)

    local read_start_ts = start_ts + TEST_INTERVAL * 2
    local read_end_ts = start_ts + TEST_INTERVAL * 5
    -- local expected_read_count = 4 -- records from index 2 to 5 (inclusive) -- Not used

    local read_batch = {}
    function read_batch:fromBinary(binary_data)
        self.binary_data = binary_data
    end
    df:read(read_batch, read_start_ts, read_end_ts)

    local expected_binary = string.sub(batch_to_write:toBinary(),
                                        (read_start_ts - start_ts) / TEST_INTERVAL * TEST_RECORD_SIZE + 1,
                                        (read_end_ts - start_ts) / TEST_INTERVAL * TEST_RECORD_SIZE + TEST_RECORD_SIZE)
    assert(expected_binary == read_batch.binary_data, "Read data mismatch for partial range")

    teardown()
end

function t_DataFile.test_read_beyond_file_boundaries()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()

    local start_ts = 1000
    local end_ts = 1000 + TEST_INTERVAL * 4
    local count = 5
    local batch_to_write = create_dummy_batch(start_ts, end_ts, count, TEST_RECORD_SIZE)
    df:write(batch_to_write)

    local read_batch = {}
    function read_batch:fromBinary(binary_data)
        self.binary_data = binary_data
    end

    -- Read before start_time and after end_time
    df:read(read_batch, start_ts - TEST_INTERVAL, end_ts + TEST_INTERVAL)
    assert(batch_to_write:toBinary() == read_batch.binary_data, "Read data mismatch when reading beyond boundaries")

    -- Read completely outside
    read_batch.binary_data = nil
    df:read(read_batch, end_ts + TEST_INTERVAL, end_ts + TEST_INTERVAL * 2)
    assert(read_batch.binary_data == nil, "Should not read data when completely outside range")

    teardown()
end

function t_DataFile.test_count()
    setup()
    local df = DataFile.new(TEST_FILE, TEST_BLOCK_SIZE, TEST_INTERVAL, TEST_RECORD_SIZE)
    df:create()
    assert(0 == df:count(), "Count should be 0 for empty file")

    local start_ts = 1000
    local end_ts = 1000 + TEST_INTERVAL * 4
    local count = 5
    local batch_to_write = create_dummy_batch(start_ts, end_ts, count, TEST_RECORD_SIZE)
    df:write(batch_to_write)
    assert(count == df:count(), "Count mismatch after writing data")

    teardown()
end

return t_DataFile