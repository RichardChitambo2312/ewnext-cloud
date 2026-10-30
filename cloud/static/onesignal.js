(function () {
  if (!document.body || document.body.dataset.push !== "1") return;

  function optIn() {
    window.OneSignalDeferred = window.OneSignalDeferred || [];
    window.OneSignalDeferred.push(async function (OneSignal) {
      try {
        await OneSignal.Notifications.requestPermission();
        if (OneSignal.User && OneSignal.User.PushSubscription) {
          await OneSignal.User.PushSubscription.optIn();
        }
      } catch (err) {}
    });
  }

  window.keyviewOneSignalOptIn = optIn;

  fetch("/api/onesignal", { credentials: "same-origin" })
    .then(function (res) {
      return res.ok ? res.json() : null;
    })
    .then(function (cfg) {
      if (!cfg || !cfg.enabled || !cfg.appId) return;
      window.OneSignalDeferred = window.OneSignalDeferred || [];
      var script = document.createElement("script");
      script.src = "https://cdn.onesignal.com/sdks/web/v16/OneSignalSDK.page.js";
      script.defer = true;
      script.onload = function () {
        window.OneSignalDeferred.push(async function (OneSignal) {
          await OneSignal.init({
            appId: cfg.appId,
            serviceWorkerPath: "/push/onesignal/OneSignalSDKWorker.js",
            serviceWorkerParam: { scope: "/push/onesignal/" },
            welcomeNotification: { disable: true },
            notifyButton: { enable: false },
          });
          if (cfg.externalId) await OneSignal.login(String(cfg.externalId));
          var wanted = {};
          (cfg.tags || []).forEach(function (tag) {
            wanted[tag] = "1";
          });
          var current = {};
          try {
            current = (await OneSignal.User.getTags()) || {};
          } catch (err) {
            current = {};
          }
          var remove = Object.keys(current).filter(function (key) {
            return key.indexOf("site_") === 0 && !wanted[key];
          });
          if (remove.length) await OneSignal.User.removeTags(remove);
          if (Object.keys(wanted).length) await OneSignal.User.addTags(wanted);
          if (Notification.permission === "granted") optIn();
        });
      };
      document.head.appendChild(script);
    })
    .catch(function () {});
})();
