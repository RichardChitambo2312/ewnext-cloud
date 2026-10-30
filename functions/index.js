const { onRequest } = require("firebase-functions/v2/https");
const { defineSecret } = require("firebase-functions/params");
const nodemailer = require("nodemailer");

const smtpPassword = defineSecret("SMTP_PASSWORD");
const relaySecret = defineSecret("MAIL_RELAY_SECRET");

const SMTP_HOST = "keyview.co.za";
const SMTP_PORT = 465;
const SMTP_USER = "notifications@keyview.co.za";

function sameSecret(got, expected) {
  if (!got || !expected || got.length !== expected.length) {
    return false;
  }
  let diff = 0;
  for (let i = 0; i < expected.length; i += 1) {
    diff |= got.charCodeAt(i) ^ expected.charCodeAt(i);
  }
  return diff === 0;
}

function cleanRecipients(value) {
  const list = Array.isArray(value) ? value : [];
  const out = [];
  for (const item of list) {
    const mail = String(item || "").trim().toLowerCase();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(mail)) {
      continue;
    }
    if (!out.includes(mail)) {
      out.push(mail);
    }
    if (out.length >= 20) {
      break;
    }
  }
  return out;
}

exports.sendMail = onRequest(
  {
    region: "us-central1",
    cors: false,
    invoker: "public",
    timeoutSeconds: 30,
    memory: "256MiB",
    maxInstances: 2,
    secrets: [smtpPassword, relaySecret],
  },
  async (req, res) => {
    if (req.method !== "POST") {
      res.status(405).send("method");
      return;
    }
    const header = String(req.get("authorization") || "");
    const token = header.startsWith("Bearer ") ? header.slice(7) : "";
    if (!sameSecret(token, relaySecret.value())) {
      res.status(401).send("unauthorized");
      return;
    }
    const body = req.body || {};
    const recipients = cleanRecipients(body.to);
    const subject = String(body.subject || "").slice(0, 200);
    const text = String(body.text || "").slice(0, 100000);
    const html = String(body.html || "").slice(0, 200000);
    if (!recipients.length || !subject) {
      res.status(400).send("bad request");
      return;
    }
    try {
      const transport = nodemailer.createTransport({
        host: SMTP_HOST,
        port: SMTP_PORT,
        secure: true,
        auth: { user: SMTP_USER, pass: smtpPassword.value() },
        connectionTimeout: 10000,
        greetingTimeout: 10000,
        socketTimeout: 20000,
      });
      await transport.sendMail({
        from: SMTP_USER,
        to: recipients,
        subject,
        text: text || subject,
        html: html || undefined,
      });
      console.log("email ok 465");
      res.status(200).json({ ok: true });
    } catch (err) {
      console.log(`email fail ${err && err.name ? err.name : "Error"}`);
      res.status(502).json({ ok: false });
    }
  }
);
