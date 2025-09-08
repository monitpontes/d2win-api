
self.addEventListener('push', function (event) {
  let data = {};
  try { data = event.data.json(); } catch (e) {}
  const title = data.title || 'Alerta';
  const body  = data.body  || 'Você tem um alerta.';
  event.waitUntil(self.registration.showNotification(title, { body, icon: undefined, data }));
});
self.addEventListener('notificationclick', function (event) {
  event.notification.close();
  event.waitUntil(clients.openWindow('/public/')); // abre dashboard (ajuste para sua URL)
});
