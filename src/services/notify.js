
import webpush from "web-push";
import twilio from "twilio";
import PushSub from "../models/pushSub.js";

const VAPID_PUBLIC_KEY  = process.env.VAPID_PUBLIC_KEY;
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY;
const VAPID_SUBJECT     = process.env.VAPID_SUBJECT || "mailto:admin@example.com";

if (VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY) {
  webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
}

const twilioSid   = process.env.TWILIO_ACCOUNT_SID;
const twilioToken = process.env.TWILIO_AUTH_TOKEN;
const twilioFrom  = process.env.TWILIO_FROM;
const twilioClient = (twilioSid && twilioToken) ? twilio(twilioSid, twilioToken) : null;

export function getVapidPublicKey() { return VAPID_PUBLIC_KEY || ""; }

export async function sendWebPushToRecipient(recipientId, payload) {
  const subs = await PushSub.find({ recipient_id: recipientId });
  const results = [];
  for (const s of subs) {
    try {
      await webpush.sendNotification({ endpoint: s.endpoint, keys: s.keys }, JSON.stringify(payload));
      results.push({ endpoint: s.endpoint, ok: true });
    } catch (e) {
      results.push({ endpoint: s.endpoint, ok: false, error: e.message });
      if (e.statusCode === 404 || e.statusCode === 410) {
        await PushSub.deleteOne({ _id: s._id });
      }
    }
  }
  return results;
}

export async function sendSMS(phone, body) {
  if (!twilioClient || !twilioFrom) {
    console.log("Twilio not configured; SMS skipped:", phone, body);
    return { ok: false, skipped: true };
  }
  const msg = await twilioClient.messages.create({ from: twilioFrom, to: phone, body });
  return { ok: true, sid: msg.sid };
}
