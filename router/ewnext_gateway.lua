-- VuCI FunctionService API. Telemetry uses Data to Server HTTP, not an API key.
local FunctionService = require("api/FunctionService")
local uci = require("uci").cursor()
local M = FunctionService:new()

local function getopt(name, default)
    return uci:get("ewnext_gateway", "main", name) or default
end
local function bool(v)
    return (v == true or v == "1" or v == 1) and "1" or "0"
end
local function int(v, lo, hi)
    local n = tonumber(v)
    if not n or n % 1 ~= 0 or n < lo or n > hi then return nil end
    return tostring(n)
end
local function valid_url(url)
    return type(url) == "string" and #url <= 256
        and url:match("^https://[%w%.%-]+[:%d]*/?[%w%p]*$") ~= nil
        and not url:find("[%s]")
        and not url:find('"', 1, true)
        and not url:find("'", 1, true)
        and not url:find("`", 1, true)
end
local function valid_device_id(id)
    if id == nil then return false end
    id = tostring(id):gsub("%s+", "")
    local n = #id
    if n < 2 or n > 32 then return false end
    return id:match("^[%w._%-]+$") ~= nil
end
local function saveopt(name, value)
    uci:set("ewnext_gateway", "main", name, tostring(value))
end

local function worker_status()
    local f = io.open("/tmp/ewnext_gateway.state.json", "r")
    if not f then return { running = false } end
    local raw = f:read("*a") or ""
    f:close()
    local ok, json = pcall(require, "luci.jsonc")
    if not ok or not json then return { running = true } end
    local obj = json.parse(raw)
    if type(obj) ~= "table" then return { running = true } end
    obj.running = true
    return obj
end

function M:GET_TYPE_config()
    return self:ResponseOK({
        device_id = getopt("device_id", ""),
        api_url = getopt("api_url", "https://ewnext-cloud.onrender.com"),
        cloud_enabled = getopt("cloud_enabled", "0") == "1",
        remote_reads_enabled = getopt("remote_reads_enabled", "0") == "1",
        writes_enabled = false,
        serial_device = getopt("serial_device", "/dev/rs485"),
        baud_rate = getopt("baud_rate", "9600"),
        poll_interval = tonumber(getopt("poll_interval", "10")),
        stale_after_seconds = tonumber(getopt("stale_after_seconds", "25")),
        worker = worker_status()
    })
end

function M:SaveConfig()
    local a = {}
    local args = self.arguments or {}
    for k, v in pairs(args) do
        if k ~= "data" then a[k] = v end
    end
    if type(args.data) == "table" then
        for k, v in pairs(args.data) do
            a[k] = v
        end
    end
    if type(a.api_url) == "string" then
        a.api_url = a.api_url:gsub("%s+", "")
        a.api_url = a.api_url:gsub("/+$", "")
        a.api_url = a.api_url:gsub("/api/modbus$", "")
    end
    if a.device_id ~= nil then
        a.device_id = tostring(a.device_id):gsub("%s+", "")
    end

    if not valid_device_id(a.device_id) then
        return self:ResponseError({ error = "Device ID must be 2–32 characters: letters, numbers, . _ -" })
    end

    if not valid_url(a.api_url) then
        return self:ResponseError({ error = "API URL must be HTTPS and contain no spaces or quotes" })
    end

    if a.serial_device == nil or a.serial_device == "" then
        a.serial_device = getopt("serial_device", "/dev/rs485")
    end
    if a.serial_device ~= "/dev/rs485" and a.serial_device ~= "/dev/ttyUSB0" then
        return self:ResponseError({ error = "Unsupported serial device" })
    end

    local baud = tostring(a.baud_rate or getopt("baud_rate", "9600"))
    if baud ~= "9600" and baud ~= "19200" and baud ~= "38400" then
        return self:ResponseError({ error = "Unsupported baud rate" })
    end

    local poll = int(a.poll_interval or getopt("poll_interval", "10"), 5, 300)
    local stale = int(a.stale_after_seconds or getopt("stale_after_seconds", "25"), 10, 3600)
    if not poll or not stale then
        return self:ResponseError({ error = "Poll interval must be 5–300; stale threshold 10–3600" })
    end

    saveopt("device_id", a.device_id)
    saveopt("api_url", a.api_url)
    saveopt("serial_device", a.serial_device)
    saveopt("baud_rate", baud)
    saveopt("poll_interval", poll)
    saveopt("stale_after_seconds", stale)
    saveopt("cloud_enabled", bool(a.cloud_enabled))
    saveopt("remote_reads_enabled", bool(a.remote_reads_enabled))
    saveopt("writes_enabled", "0")

    uci:save("ewnext_gateway")
    uci:commit("ewnext_gateway")
    os.execute("chmod 600 /etc/config/ewnext_gateway")
    os.execute("/etc/init.d/ewnext_gateway reload >/dev/null 2>&1")

    return self:ResponseOK({ saved = true, device_id = a.device_id })
end

local save_action = M:action("save", M.SaveConfig)
local function opt(name, required)
    local o = save_action:option(name)
    if required then o.require = true end
    return o
end
opt("device_id", false)
opt("api_url", false)
opt("api_key", false)
opt("cloud_enabled", false)
opt("remote_reads_enabled", false)
opt("serial_device", false)
opt("baud_rate", false)
opt("poll_interval", false)
opt("stale_after_seconds", false)
local data = save_action:option("data")
data.require = false
return M
