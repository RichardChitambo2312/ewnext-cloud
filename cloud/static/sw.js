self.addEventListener("install", function () {
  self.skipWaiting();
});

self.addEventListener("activate", function (event) {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("push", function (event) {
  var data = {};
  try {
    data = event.data ? event.data.json() : {};
  } catch (err) {
    data = { title: "Keyview", body: event.data ? event.data.text() : "" };
  }
  var title = data.title || "Keyview";
  var body = data.body || "Keyview alert";
  var url = data.url || "/";
  var tag = data.tag || "keyview";
  var icon = new URL("/static/brand/icon-192.png", self.location.origin).href;
  event.waitUntil(
    self.registration.showNotification(title, {
      body: body,
      icon: icon,
      tag: tag,
      data: { url: url }
    }).catch(function () {
      return self.registration.showNotification(title, { body: body });
    })
  );
});

self.addEventListener("notificationclick", function (event) {
  event.notification.close();
  var target = (event.notification.data && event.notification.data.url) || "/";
  if (target.indexOf("http") !== 0) target = self.location.origin + target;
  event.waitUntil(
    self.clients.matchAll({ type: "window", includeUncontrolled: true }).then(function (windows) {
      for (var i = 0; i < windows.length; i++) {
        if (windows[i].url.indexOf(target) !== -1 && "focus" in windows[i]) {
          return windows[i].focus();
        }
      }
      if (self.clients.openWindow) return self.clients.openWindow(target);
    })
  );
});
