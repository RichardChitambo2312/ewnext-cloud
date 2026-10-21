#!/usr/bin/lua
-- EWNext gateway worker. Reads UCI ewnext_gateway + /tmp/modbus_db.
-- Cloud publish only when cloud_enabled=1 and an API key is set.
-- Remote reads only when remote_reads_enabled=1. Register writes are always refused.

local sqlite = require("lsqlite3")
local json = require("luci.jsonc")
local uci_mod = require("uci")

local STATE_PATH = "/tmp/ewnext_gateway.state.json"
local BODY_PATH = "/tmp/ewnext_gateway.body"
local HDR_PATH = "/tmp/ewnext_gateway.hdr"
local RESP_PATH = "/tmp/ewnext_gateway.resp"
local DB_PATH = "/tmp/modbus_db"
local WRITE_FC = { [5] = true, [6] = true, [15] = true, [16] = true }

local function log(msg)
    os.execute(string.format("logger -t ewnext_gateway %q", tostring(msg)))
end

local function now()
    return os.time()
end

local function read_cfg()
    local x = uci_mod.cursor()
    local g = "ewnext_gateway"
    local s = "main"
    local function get(k, d)
        return x:get(g, s, k) or d
    end
    local hostname = "TRB245"
    local p = io.popen("uci -q get system.@system[0].hostname")
    if p then
        hostname = p:read("*l") or hostname
        p:close()
    end
    return {
        api_url = get("api_url", "https://modbus-api-production-3c15.up.railway.app"),
        api_key = get("api_key", ""),
        cloud_enabled = get("cloud_enabled", "0") == "1",
        remote_reads_enabled = get("remote_reads_enabled", "0") == "1",
        writes_enabled = false,
        serial_device = get("serial_device", "/dev/rs485"),
        baud_rate = tonumber(get("baud_rate", "9600")) or 9600,
        poll_interval = tonumber(get("poll_interval", "10")) or 10,
        stale_after_seconds = tonumber(get("stale_after_seconds", "25")) or 25,
        device_id = get("device_id", hostname),
        telemetry_path = get("telemetry_path", "/api/modbus")
    }
end

local function write_state(st)
    local f = io.open(STATE_PATH, "w")
    if not f then return end
    f:write(json.stringify(st) or "{}")
    f:close()
end

local function shellquote(s)
    return "'" .. tostring(s):gsub("'", "'\\''") .. "'"
end

local function latest_readings(stale_after)
    local out = {}
    local db = sqlite.open(DB_PATH, sqlite.OPEN_READONLY)
    if not db then db = sqlite.open(DB_PATH) end
    if not db then return out end
    local sql = [[
        SELECT unit_id, server_name, request_name, function_code, start_address,
               data_count, time, response_data
        FROM modbus_data
        WHERE id IN (SELECT MAX(id) FROM modbus_data GROUP BY unit_id)
    ]]
    local tnow = now()
    for row in db:nrows(sql) do
        local values = json.parse(row.response_data or "[]") or {}
        local age = tnow - (tonumber(row.time) or 0)
        out[#out + 1] = {
            instrument_id = tonumber(row.unit_id),
            server_name = row.server_name,
            request_name = row.request_name,
            function_code = tonumber(row.function_code),
            start_address = tonumber(row.start_address),
            values = values,
            timestamp = tonumber(row.time),
            age_seconds = age,
            stale = age > stale_after
        }
    end
    db:close()
    return out
end

local function http_post(url, key, body)
    local bf = io.open(BODY_PATH, "w")
    if not bf then return 0, "cannot write body" end
    bf:write(body)
    bf:close()
    os.execute("chmod 600 " .. BODY_PATH)

    local hf = io.open(HDR_PATH, "w")
    if not hf then return 0, "cannot write headers" end
    hf:write("Content-Type: application/json\n")
    if key and #key > 0 then
        hf:write("X-API-Key: " .. key .. "\n")
        hf:write("Authorization: Bearer " .. key .. "\n")
    end
    hf:close()
    os.execute("chmod 600 " .. HDR_PATH)

    local cmd = string.format(
        "curl -sS -m 20 -X POST %s -H @%s --data-binary @%s -o %s -w '%%{http_code}' 2>/tmp/ewnext_gateway.curlerr",
        shellquote(url), HDR_PATH, BODY_PATH, RESP_PATH
    )
    local p = io.popen(cmd, "r")
    local code = p and p:read("*l") or "0"
    if p then p:close() end
    os.remove(HDR_PATH)
    os.remove(BODY_PATH)
    local rf = io.open(RESP_PATH, "r")
    local resp = rf and rf:read("*a") or ""
    if rf then rf:close() end
    os.remove(RESP_PATH)
    return tonumber(code) or 0, resp
end

local function join_url(base, path)
    base = (base or ""):gsub("/+$", "")
    if not path:match("^/") then path = "/" .. path end
    return base .. path
end

local function refuse_write(job)
    log(string.format("refusing write job fc=%s addr=%s (writes locked)",
        tostring(job["function"] or job.function_code),
        tostring(job.address or job.start_address)))
    return { ok = false, error = "writes_locked" }
end

local function is_write_job(job)
    local fc = tonumber(job["function"] or job.function_code or 0) or 0
    local kind = tostring(job.type or job.action or ""):lower()
    return WRITE_FC[fc] or kind == "write" or kind == "fc5" or kind == "fc6" or kind == "fc15" or kind == "fc16"
end

local function handle_cloud_reply(cfg, resp)
    if not cfg.remote_reads_enabled then return end
    local obj = json.parse(resp or "")
    if type(obj) ~= "table" then return end
    local jobs = obj.reads or obj.commands or obj.jobs
    if type(jobs) ~= "table" then return end
    for _, job in ipairs(jobs) do
        if type(job) == "table" then
            if is_write_job(job) then
                refuse_write(job)
            else
                -- Remote reads are served from the already-polled cache in this version
                -- so we never take the RS485 bus from Teltonika modbus_client.
                log(string.format("remote read requested fc=%s addr=%s (served from cache, no bus write)",
                    tostring(job["function"] or job.function_code),
                    tostring(job.address or job.start_address)))
            end
        end
    end
end

local function publish(cfg, readings)
    if not cfg.cloud_enabled then
        return false, "cloud_disabled"
    end
    local url = join_url(cfg.api_url, cfg.telemetry_path)
    local last_err = "no_instruments"
    local ok_any = false
    for _, r in ipairs(readings) do
        local payload = {
            values = r.values,
            instrument_id = r.instrument_id,
            device_id = cfg.device_id,
            timestamp = r.timestamp,
            stale = r.stale,
            server_name = r.server_name,
            request_name = r.request_name,
            writes_enabled = false
        }
        local code, resp = http_post(url, cfg.api_key, json.stringify(payload) or "{}")
        if code >= 200 and code < 300 then
            ok_any = true
            handle_cloud_reply(cfg, resp)
        else
            last_err = "http_" .. tostring(code)
            log("publish failed instrument=" .. tostring(r.instrument_id) .. " " .. last_err)
        end
    end
    if ok_any then return true, "ok" end
    return false, last_err
end

log("worker start")
while true do
    local cfg = read_cfg()
    if cfg.poll_interval < 5 then cfg.poll_interval = 5 end
    if cfg.poll_interval > 300 then cfg.poll_interval = 300 end

    local readings = latest_readings(cfg.stale_after_seconds)
    local ok, err = true, "idle"
    if cfg.cloud_enabled then
        ok, err = publish(cfg, readings)
    else
        err = "cloud_disabled"
    end

    local slim = {}
    for _, r in ipairs(readings) do
        slim[#slim + 1] = {
            instrument_id = r.instrument_id,
            stale = r.stale,
            age_seconds = r.age_seconds
        }
    end

    write_state({
        ts = now(),
        cloud_enabled = cfg.cloud_enabled,
        remote_reads_enabled = cfg.remote_reads_enabled,
        writes_locked = true,
        api_key_set = cfg.api_key ~= "",
        last_error = err,
        last_ok = ok,
        instrument_count = #readings,
        instruments = slim
    })

    os.execute("sleep " .. tostring(cfg.poll_interval))
end
