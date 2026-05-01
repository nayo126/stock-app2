// 自己削除型 Service Worker - PWA cache 完全解除
self.addEventListener("install", (event) => {
  event.waitUntil(
    Promise.all([
      caches.keys().then(keys => Promise.all(keys.map(k => caches.delete(k)))),
      self.registration.unregister(),
    ]).then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    self.clients.matchAll().then(clients => {
      clients.forEach(client => client.navigate(client.url));
    })
  );
});

self.addEventListener("fetch", (event) => {
  event.respondWith(fetch(event.request).catch(() => new Response("offline", {status: 503})));
});
