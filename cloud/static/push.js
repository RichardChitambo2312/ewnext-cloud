(function () {
  if (!("serviceWorker" in navigator)) return;

  navigator.serviceWorker.register("/sw.js").catch(function () {});

  if (document.body.dataset.push !== "1") return;

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

  function subscribe() {
    return navigator.serviceWorker.ready.then(function (reg) {
      return fetch("/api/push/public-key", { credentials: "same-origin" }).then(function (res) {
        if (!res.ok) throw new Error("key");
        return res.json();
      }).then(function (data) {
        return reg.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey: urlBase64ToBytes(data.publicKey)
        });
      }).then(function (sub) {
        return fetch("/api/push/subscribe", {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(sub)
        });
      });
    });
  }

  if (later) {
    later.addEventListener("click", function () {
      sessionStorage.setItem("keyview-push-later", "1");
      hide();
    });
  }

  if (ios() && !standalone()) {
    show("Add Keyview to your Home Screen, open it from the icon, then turn on alerts.", false);
    return;
  }

  if (!("Notification" in window) || !("PushManager" in window)) return;

  if (Notification.permission === "granted") {
    subscribe().catch(function () {});
    return;
  }

  if (Notification.permission === "denied") {
    show("Alerts are blocked. Allow notifications for Keyview in the phone settings.", false);
    return;
  }

  show("Turn on alerts for alarms, defrost, and controllers going offline.", true);
  if (!allow) return;
  allow.addEventListener("click", function () {
    allow.disabled = true;
    Notification.requestPermission().then(function (result) {
      if (result !== "granted") {
        allow.disabled = false;
        show("Alerts stay off until you allow notifications.", false);
        return;
      }
      subscribe().then(function () {
        hide();
      }).catch(function () {
        allow.disabled = false;
        show("Alerts could not be turned on. Open Keyview from the home screen and try again.", true);
      });
    });
  });
})();
