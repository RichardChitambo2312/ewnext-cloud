const w = (s, e) => {
  const i = s.__vccOpts || s;
  for (const [d, t] of e)
    i[d] = t;
  return i;
}, V = {
  name: "EwNextGateway",
  data() {
    return {
      busy: !1,
      form: {
        device_id: "",
        api_url: "https://ewnext-cloud.onrender.com",
        cloud_enabled: !1,
        remote_reads_enabled: !1,
        serial_device: "/dev/rs485",
        baud_rate: "9600",
        poll_interval: "10",
        stale_after_seconds: "25"
      },
      deviceOptions: [
        { label: "/dev/rs485", value: "/dev/rs485" },
        { label: "/dev/ttyUSB0", value: "/dev/ttyUSB0" }
      ],
      baudOptions: [
        { label: "9600", value: "9600" },
        { label: "19200", value: "19200" },
        { label: "38400", value: "38400" }
      ]
    };
  },
  async mounted() {
    try {
      const s = await this.$axios.get("/api/ewnext_gateway_f/config"), e = (s.data && s.data.data) || s.data || s;
      this.form.device_id = e.device_id || this.form.device_id;
      this.form.api_url = e.api_url || this.form.api_url;
      this.form.cloud_enabled = !!e.cloud_enabled;
      this.form.remote_reads_enabled = !!e.remote_reads_enabled;
      this.form.serial_device = e.serial_device || this.form.serial_device;
      this.form.baud_rate = String(e.baud_rate || this.form.baud_rate);
      this.form.poll_interval = String(e.poll_interval || this.form.poll_interval);
      this.form.stale_after_seconds = String(e.stale_after_seconds || this.form.stale_after_seconds);
    } catch {
      this.$message.error("Could not load EWNext Gateway settings");
    }
  },
  methods: {
    normalizeUrl(url) {
      let u = String(url || "").trim();
      u = u.replace(/\/+$/, "");
      u = u.replace(/\/api\/modbus$/i, "");
      return u;
    },
    async save(ev) {
      if (ev && ev.preventDefault) ev.preventDefault();
      if (ev && ev.stopPropagation) ev.stopPropagation();
      const deviceId = String(this.form.device_id || "").trim();
      this.form.device_id = deviceId;
      if (!/^[A-Za-z0-9][A-Za-z0-9._-]{1,31}$/.test(deviceId)) {
        this.$message.error("Device ID must be 2–32 characters: letters, numbers, . _ -");
        return;
      }
      const url = this.normalizeUrl(this.form.api_url);
      this.form.api_url = url;
      if (!url.startsWith("https://") || /[\s\"'`]/.test(url)) {
        this.$message.error("API endpoint must use HTTPS and contain no spaces or quotes");
        return;
      }
      const s = Number(this.form.poll_interval), e = Number(this.form.stale_after_seconds);
      if (!Number.isInteger(s) || s < 5 || s > 300) {
        this.$message.error("Poll interval must be 5–300 seconds");
        return;
      }
      if (!Number.isInteger(e) || e < 10 || e > 3600) {
        this.$message.error("Stale threshold must be 10–3600 seconds");
        return;
      }
      this.busy = !0;
      try {
        const resp = await this.$axios.post("/api/ewnext_gateway_f/actions/save", {
          device_id: deviceId,
          api_url: url,
          cloud_enabled: this.form.cloud_enabled,
          remote_reads_enabled: this.form.remote_reads_enabled,
          serial_device: this.form.serial_device || "/dev/rs485",
          baud_rate: String(this.form.baud_rate || "9600"),
          poll_interval: s,
          stale_after_seconds: e,
          data: {
            device_id: deviceId,
            api_url: url,
            cloud_enabled: this.form.cloud_enabled,
            remote_reads_enabled: this.form.remote_reads_enabled,
            serial_device: this.form.serial_device || "/dev/rs485",
            baud_rate: String(this.form.baud_rate || "9600"),
            poll_interval: s,
            stale_after_seconds: e
          }
        });
        const body = (resp && resp.data) || resp;
        if (body && body.success === false) {
          const err = (body.errors && body.errors[0] && body.errors[0].error) || body.error || "Save rejected";
          this.$message.error(String(err));
          return;
        }
        this.$message.success("EWNext Gateway settings saved");
      } catch (err) {
        const d = err && err.response && err.response.data;
        const msg = (d && d.errors && d.errors[0] && d.errors[0].error) || (d && d.error) || (err && err.message) || "Could not save settings";
        this.$message.error(String(msg));
      } finally {
        this.busy = !1;
      }
    }
  }
}, r = window.Vue.resolveComponent, l = window.Vue.createVNode, p = window.Vue.createElementVNode, n = window.Vue.withCtx, y = window.Vue.toDisplayString, g = window.Vue.createTextVNode, h = window.Vue.openBlock, k = window.Vue.createElementBlock;
function x(s, e, i, d, t, f) {
  const a = r("tlt-form-item-input"), u = r("tlt-form-item-switch"), m = r("tlt-form-item-select"), _ = r("tlt-form-model-item"), c = r("tlt-button"), b = r("tlt-form"), v = r("tlt-card");
  return h(), k("div", null, [
    l(v, { title: "Cloud connection" }, {
      default: n(() => [
        l(b, {
          ref: "settingsForm",
          model: t.form,
          "custom-save": !0,
          sid: "ewnext_gateway",
          title: "EWNext Gateway"
        }, {
          default: n(() => [
            l(_, { label: "Telemetry" }, {
              default: n(() => [...e[0] || (e[0] = [
                p("p", null, "Live values are sent from Services → Data to Server. Use Type HTTP, Server address https://ewnext-cloud.onrender.com/api/modbus, header Content-Type: application/json, and Enable secure connection. This page does not use a Router API key.", -1)
              ])]),
              _: 1
            }),
            l(a, {
              modelValue: t.form.device_id,
              "onUpdate:modelValue": e[9] || (e[9] = (o) => t.form.device_id = o),
              label: "Device ID (site identifier)",
              prop: "device_id",
              maxlength: "32"
            }, null, 8, ["modelValue"]),
            l(_, { label: "Site identity" }, {
              default: n(() => [...e[10] || (e[10] = [
                p("p", null, "This unique Device ID identifies the site in the cloud. Each site can have up to 15 instruments.", -1)
              ])]),
              _: 1
            }),
            l(a, {
              modelValue: t.form.api_url,
              "onUpdate:modelValue": e[1] || (e[1] = (o) => t.form.api_url = o),
              label: "HTTPS API endpoint",
              prop: "api_url",
              maxlength: "256"
            }, null, 8, ["modelValue"]),
            l(u, {
              modelValue: t.form.cloud_enabled,
              "onUpdate:modelValue": e[2] || (e[2] = (o) => t.form.cloud_enabled = o),
              label: "Enable cloud publishing"
            }, null, 8, ["modelValue"]),
            l(u, {
              modelValue: t.form.remote_reads_enabled,
              "onUpdate:modelValue": e[3] || (e[3] = (o) => t.form.remote_reads_enabled = o),
              label: "Allow remote register reads"
            }, null, 8, ["modelValue"]),
            l(m, {
              modelValue: t.form.serial_device,
              "onUpdate:modelValue": e[4] || (e[4] = (o) => t.form.serial_device = o),
              label: "Serial device",
              options: t.deviceOptions
            }, null, 8, ["modelValue", "options"]),
            l(m, {
              modelValue: t.form.baud_rate,
              "onUpdate:modelValue": e[5] || (e[5] = (o) => t.form.baud_rate = o),
              label: "Baud rate",
              options: t.baudOptions
            }, null, 8, ["modelValue", "options"]),
            l(a, {
              modelValue: t.form.poll_interval,
              "onUpdate:modelValue": e[6] || (e[6] = (o) => t.form.poll_interval = o),
              label: "Poll interval (5–300 seconds)",
              prop: "poll_interval"
            }, null, 8, ["modelValue"]),
            l(a, {
              modelValue: t.form.stale_after_seconds,
              "onUpdate:modelValue": e[7] || (e[7] = (o) => t.form.stale_after_seconds = o),
              label: "Stale threshold (10–3600 seconds)",
              prop: "stale_after_seconds"
            }, null, 8, ["modelValue"]),
            l(_, { label: "Register writes" }, {
              default: n(() => [...e[8] || (e[8] = [
                p("strong", null, "Locked off", -1),
                p("p", null, "Register writes cannot be enabled from this page.", -1)
              ])]),
              _: 1
            }),
            l(_, { label: "Save" }, {
              default: n(() => [
                l(c, {
                  type: "button",
                  disabled: t.busy,
                  onClick: f.save
                }, {
                  default: n(() => [
                    g(y(t.busy ? "Saving…" : "Save configuration"), 1)
                  ]),
                  _: 1
                }, 8, ["disabled", "onClick"])
              ]),
              _: 1
            })
          ]),
          _: 1
        }, 8, ["model"])
      ]),
      _: 1
    })
  ]);
}
const N = /* @__PURE__ */ w(V, [["render", x]]);
export {
  N as default
};
