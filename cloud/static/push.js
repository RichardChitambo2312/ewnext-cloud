(function () {
  if (!("serviceWorker" in navigator)) return;

  function whenActive() {
    return navigator.serviceWorker.register("/sw.js", { updateViaCache: "none" }).then(function (reg) {
      var worker = reg.installing || reg.waiting;
      if (!worker) return navigator.serviceWorker.ready;
      return new Promise(function (resolve) {
        worker.addEventListener("statechange", function () {
          if (worker.state === "activated") resolve(reg);
        });
        if (worker.state === "activated") resolve(reg);
      });
    });
  }

  if (document.body.dataset.push !== "1") {
    whenActive().catch(function () {});
    return;
  }

  var banner = document.getElementById("push-banner");
  var text = document.getElementById("push-banner-text");
  var allow = document.getElementById("push-allow");
  var later = document.getElementById("push-later");
  if (!banner || !text) return;

  function ios() {
    return /iphone|ipad|ipod/i.test(navigator.userAgent);
  }

  function standalone() {
    return window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;
  }

  function hide() {
    banner.hidden = true;
  }

  function show(message, withButton) {
    if (sessionStorage.getItem("keyview-push-later") === "1") return;
    text.textContent = message;
    if (allow) allow.hidden = !withButton;
    banner.hidden = false;
  }

  function urlBase64ToBytes(value) {
    var padded = value + "=".repeat((4 - (value.length % 4)) % 4);
    var raw = atob(padded.replace(/-/g, "+").replace(/_/g, "/"));
    var out = new Uint8Array(raw.length);
    for (var i = 0; i < raw.length; i++) out[i] = raw.charCodeAt(i);
    return out;
  }

  function subscribe(welcome) {
    return whenActive().then(function (reg) {
      return fetch("/api/push/public-key", { credentials: "same-origin" }).then(function (res) {
        if (!res.ok) throw new Error("key");
        return res.json();
      }).then(function (data) {
        return reg.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey: urlBase64ToBytes(data.publicKey)
        });
      }).then(function (sub) {
        var saved = sub.toJSON ? sub.toJSON() : sub;
        saved.welcome = !!welcome;
        return fetch("/api/push/subscribe", {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(saved)
        });
      });
    });
  }

  function markReady(res) {
    if (!res || !res.ok) throw new Error("save");
    sessionStorage.setItem("keyview-push-tested", "1");
    hide();
  }

  if (later) {
    later.addEventListener("click", function () {
      sessionStorage.setItem("keyview-push-later", "1");
      hide();
    });
  }

  if (allow) {
    allow.addEventListener("click", function () {
      allow.disabled = true;
      var ask = Notification.permission === "granted"
        ? Promise.resolve("granted")
        : Notification.requestPermission();
      ask.then(function (result) {
        if (result !== "granted") {
          allow.disabled = false;
          show("Alerts stay off until you allow notifications.", false);
          return;
        }
        if (typeof window.keyviewOneSignalOptIn === "function") window.keyviewOneSignalOptIn();
        subscribe(true).then(markReady).catch(function () {
          allow.disabled = false;
          show("Alerts could not reach this phone. Stay in the home-screen app and try again.", true);
        });
      });
    });
  }

  if (ios() && !standalone()) {
    show("Add Keyview to your Home Screen, open it from the icon, then turn on alerts.", false);
    return;
  }

  if (!("Notification" in window) || !("PushManager" in window)) return;

  if (Notification.permission === "granted") {
    var tested = sessionStorage.getItem("keyview-push-tested") === "1";
    subscribe(!tested).then(function (res) {
      if (!tested) markReady(res);
      else if (!res || !res.ok) throw new Error("save");
    }).catch(function () {
      show("Alerts could not reach this phone. Tap Turn on alerts again.", true);
    });
    return;
  }

  if (Notification.permission === "denied") {
    show("Alerts are blocked. Allow notifications for Keyview in the phone settings.", false);
    return;
  }

  show("Turn on alerts for alarms, defrost, and controllers going offline.", true);
})();
