local DataTable = require("db.DataTable")
local Config = require("conf.Config")

local DB = {
    root_path = nil,
    data_tables = nil,
}

DB.__index = DB

local function get_file_path(root_path, table_name)
    return root_path .. table_name .. ".bin"
end

function DB.new(root_path, v_table_name, safe)
    local self = setmetatable({}, DB)
    self.root_path = root_path
    self.data_tables = {}
    for table_name, config in pairs(Config) do
        if not v_table_name or v_table_name == table_name then
            local path = get_file_path(self.root_path, table_name)
            self.data_tables[table_name] = DataTable.new(table_name, config, path, safe)
        end
    end
    if v_table_name and not self.data_tables[v_table_name] then
        error(string.format("Table '%s' not defined.", v_table_name))
    end
    return self
end

function DB:get_table(table_name)
    local table_val = self.data_tables[table_name]
    if not table_val then
        error("Table '" .. table_name .. "' not defined.")
    end
    return table_val
end

return DB