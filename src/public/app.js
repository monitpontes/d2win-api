
async function getVapid() {
  const r = await fetch('/push/vapidPublicKey');
  const j = await r.json();
  return j.publicKey;
}

function urlBase64ToUint8Array(base64String) {
  const padding = '='.repeat((4 - base64String.length % 4) % 4);
  const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
  const rawData = atob(base64);
  const outputArray = new Uint8Array(rawData.length);
  for (let i = 0; i < rawData.length; ++i) outputArray[i] = rawData.charCodeAt(i);
  return outputArray;
}

async function register() {
  const out = document.getElementById('out');
  if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
    out.textContent = 'Push não suportado neste navegador.';
    return;
  }
  const reg = await navigator.serviceWorker.register('/public/sw.js');
  await navigator.serviceWorker.ready;
  const sub = await reg.pushManager.subscribe({
    userVisibleOnly: true,
    applicationServerKey: urlBase64ToUint8Array(await getVapid())
  });
  const body = {
    endpoint: sub.endpoint,
    keys: sub.toJSON().keys,
    recipient_id: document.getElementById('recipientId').value || undefined,
    bridge_id: document.getElementById('bridgeId').value || undefined
  };
  const r = await fetch('/push/subscribe', { method: 'POST', headers: { 'Content-Type':'application/json' }, body: JSON.stringify(body)});
out.textContent = 'Assinado!\n' + JSON.stringify(await r.json(), null, 2);
}

document.getElementById('btn').addEventListener('click', register);
