local M = {}

local TSTable = require("TSTable")
local Schema = require("Schema")

local TSDatabase = {}
TSDatabase.__index = TSDatabase

local function getFilePath(dataPath, tableName)
    return dataPath .. tableName .. ".bin"
end

function M.new(dataPath, tableName, readOnly)
    local self = {
        dataPath = dataPath,
        tables = {},
        readOnly = readOnly,
    }

    for tblName, schemaDef in pairs(Schema) do
        if type(schemaDef) ~= 'table' or not schemaDef.schema then
            error(string.format("Table '%s' definition is invalid: missing 'schema' key.", tblName))
        end
        if not tableName or tableName == tblName then
            local filePath = getFilePath(self.dataPath, tblName)
            self.tables[tblName] = TSTable.new(schemaDef.schema, filePath, self.readOnly)
        end
    end

    if tableName and not self.tables[tableName] then
        error(string.format("Table '%s' not defined in schema module.", tableName))
    end    
    return setmetatable(self, TSDatabase)
end

function TSDatabase:getTable(tableName)
    local tsTable = self.tables[tableName]
    if not tsTable then error("Table '" .. tableName .. "' not loaded or defined in schema.") end
    return tsTable
end

function TSDatabase:scanTablesStat()
    local result = {}
    for tableName, tsTable in pairs(self.tables) do
         result[tableName] = tsTable:getStat()
    end
    return result
end

return M