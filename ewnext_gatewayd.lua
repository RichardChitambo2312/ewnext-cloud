#!/usr/bin/lua
-- EWNext gateway worker. Reads UCI ewnext_gateway + /tmp/modbus_db.
-- Publishes telemetry to the cloud and applies queued setpoint writes (FC16 @ 32769).

local sqlite = require("lsqlite3")
local json = require("luci.jsonc")
local uci_mod = require("uci")
local nixio = require("nixio")

local STATE_PATH = "/tmp/ewnext_gateway.state.json"
local BODY_PATH = "/tmp/ewnext_gateway.body"
local HDR_PATH = "/tmp/ewnext_gateway.hdr"
local RESP_PATH = "/tmp/ewnext_gateway.resp"
local DB_PATH = "/tmp/modbus_db"
local SETPOINT_REGISTER = 32769

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
        writes_enabled = get("writes_enabled", "0") == "1" or get("writes", "0") == "1",
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

local function bxor(a, b)
    local r, p = 0, 1
    a, b = math.floor(a), math.floor(b)
    while a > 0 or b > 0 do
        if (a % 2) ~= (b % 2) then r = r + p end
        a, b, p = math.floor(a / 2), math.floor(b / 2), p * 2
    end
    return r
end

local function crc16(s)
    local crc = 0xFFFF
    for i = 1, #s do
        crc = bxor(crc, s:byte(i))
        for _ = 1, 8 do
            local lsb = crc % 2
            crc = math.floor(crc / 2)
            if lsb == 1 then crc = bxor(crc, 0xA001) end
        end
    end
    return crc % 256, math.floor(crc / 256)
end

local function frame(bytes)
    local lo, hi = crc16(bytes)
    return bytes .. string.char(lo, hi)
end

local function u16(n)
    n = math.floor(tonumber(n) or 0)
    if n < 0 then n = n + 65536 end
    n = n % 65536
    return string.char(math.floor(n / 256), n % 256)
end

local function open_bus(cfg)
    os.execute(string.format(
        "stty -F %s %s cs8 -cstopb -parenb raw -echo min 0 time 10",
        shellquote(cfg.serial_device), tostring(cfg.baud_rate)
    ))
    return nixio.open(cfg.serial_device, "r+")
end

local function xfer(f, req, ns)
    f:write(req)
    nixio.nanosleep(0, ns or 250000000)
    return f:read(128) or ""
end

local function write_setpoint(cfg, slave, raw_value)
    slave = tonumber(slave)
    if not slave then return false, "bad_slave" end
    local f = open_bus(cfg)
    if not f then return false, "serial_open_failed" end
    local wr = frame(string.char(slave, 16) .. u16(SETPOINT_REGISTER) .. u16(1) .. string.char(2) .. u16(raw_value))
    local ack = xfer(f, wr, 300000000)
    nixio.nanosleep(0, 400000000)
    local rd = frame(string.char(slave, 3) .. u16(SETPOINT_REGISTER) .. u16(1))
    local resp = xfer(f, rd, 200000000)
    f:close()
    if #ack < 8 or ack:byte(1) ~= slave or ack:byte(2) ~= 16 then
        return false, "write_nack"
    end
    if #resp >= 7 and resp:byte(1) == slave and resp:byte(2) == 3 then
        local got = resp:byte(4) * 256 + resp:byte(5)
        local want = math.floor(tonumber(raw_value) or 0)
        if want < 0 then want = want + 65536 end
        want = want % 65536
        if got ~= want then
            return false, "readback_mismatch"
        end
    end
    return true, "ok"
end

local function report_job(cfg, job_id, ok, err)
    local url = join_url(cfg.api_url, "/api/modbus/jobs/" .. tostring(job_id) .. "/result")
    local body = json.stringify({ ok = ok and true or false, error = err }) or "{}"
    local code = http_post(url, cfg.api_key, body)
    if code < 200 or code >= 300 then
        log("job result http_" .. tostring(code) .. " id=" .. tostring(job_id))
    end
end

local function collect_jobs(resp, into, seen)
    local obj = json.parse(resp or "")
    if type(obj) ~= "table" then return end
    local jobs = obj.jobs or obj.commands or obj.reads
    if type(jobs) ~= "table" then return end
    for _, job in ipairs(jobs) do
        if type(job) == "table" then
            local id = tonumber(job.id)
            local kind = tostring(job.type or job.kind or ""):lower()
            if id and not seen[id] and kind == "setpoint" then
                seen[id] = true
                into[#into + 1] = job
            end
        end
    end
end

local function apply_setpoints(cfg, jobs)
    if #jobs == 0 then return 0, 0 end
    os.execute("/etc/init.d/modbus_client stop >/dev/null 2>&1")
    nixio.nanosleep(1, 0)
    local ok_n, err_n = 0, 0
    for _, job in ipairs(jobs) do
        local slave = tonumber(job.instrument_id)
        local raw = tonumber(job.value or job.raw_value)
        local ok, err = write_setpoint(cfg, slave, raw)
        report_job(cfg, job.id, ok, err)
        if ok then
            ok_n = ok_n + 1
            log(string.format("setpoint uid=%s raw=%s ok", tostring(slave), tostring(raw)))
        else
            err_n = err_n + 1
            log(string.format("setpoint uid=%s raw=%s failed %s", tostring(slave), tostring(raw), tostring(err)))
        end
    end
    os.execute("/etc/init.d/modbus_client start >/dev/null 2>&1")
    return ok_n, err_n
end

local function publish(cfg, readings)
    if not cfg.cloud_enabled then
        return false, "cloud_disabled", {}
    end
    local url = join_url(cfg.api_url, cfg.telemetry_path)
    local last_err = "no_instruments"
    local ok_any = false
    local jobs, seen = {}, {}
    for _, r in ipairs(readings) do
        local payload = {
            values = r.values,
            instrument_id = r.instrument_id,
            device_id = cfg.device_id,
            timestamp = r.timestamp,
            stale = r.stale,
            server_name = r.server_name,
            request_name = r.request_name,
            start_address = r.start_address,
            register_address = r.start_address,
            writes_enabled = true
        }
        local code, resp = http_post(url, cfg.api_key, json.stringify(payload) or "{}")
        if code >= 200 and code < 300 then
            ok_any = true
            collect_jobs(resp, jobs, seen)
        else
            last_err = "http_" .. tostring(code)
            log("publish failed instrument=" .. tostring(r.instrument_id) .. " " .. last_err)
        end
    end
    if ok_any then return true, "ok", jobs end
    return false, last_err, jobs
end

log("worker start")
while true do
    local cfg = read_cfg()
    if cfg.poll_interval < 5 then cfg.poll_interval = 5 end
    if cfg.poll_interval > 300 then cfg.poll_interval = 300 end

    local readings = latest_readings(cfg.stale_after_seconds)
    local ok, err, jobs = true, "idle", {}
    if cfg.cloud_enabled then
        ok, err, jobs = publish(cfg, readings)
        if ok and jobs and #jobs > 0 then
            local ok_n, err_n = apply_setpoints(cfg, jobs)
            err = string.format("setpoint_ok=%d err=%d", ok_n, err_n)
        end
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
        writes_locked = false,
        api_key_set = cfg.api_key ~= "",
        last_error = err,
        last_ok = ok,
        instrument_count = #readings,
        pending_jobs = jobs and #jobs or 0,
        instruments = slim
    })

    os.execute("sleep " .. tostring(cfg.poll_interval))
end
