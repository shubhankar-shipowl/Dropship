# Gmail API OAuth2 - Quick Start Guide

## ✅ What's Already Done

1. ✅ SMTP configuration added to `.env`
2. ✅ Gmail API OAuth2 code implemented
3. ✅ Helper endpoints created
4. ✅ Step-by-step guide created (`GMAIL_API_SETUP.md`)

## 🚀 Quick Setup (5 Steps)

### Step 1: Get OAuth2 Credentials

1. Go to: https://console.cloud.google.com/
2. Create project → Enable Gmail API → Create OAuth 2.0 credentials
3. Application type: **Desktop app**
4. Copy **Client ID** and **Client Secret**

### Step 2: Add to .env

```env
GMAIL_CLIENT_ID=your_client_id_here
GMAIL_CLIENT_SECRET=your_client_secret_here
GMAIL_REDIRECT_URI=urn:ietf:wg:oauth:2.0:oob
```

### Step 3: Get Authorization URL

```bash
# Restart server first
pm2 restart dropshipper-payout-app

# Then visit in browser:
http://localhost:3007/api/gmail-oauth-setup
```

### Step 4: Authorize & Get Code

1. Open the `authUrl` from Step 3
2. Sign in with: `shubhankarhaldar07@gmail.com`
3. Click "Allow"
4. Copy the authorization code

### Step 5: Get Refresh Token

```bash
curl -X POST http://localhost:3007/api/gmail-oauth-callback \
  -H "Content-Type: application/json" \
  -d '{"code":"YOUR_AUTHORIZATION_CODE_HERE"}'
```

Copy the `refreshToken` and add to `.env`:

```env
GMAIL_REFRESH_TOKEN=your_refresh_token_here
```

### Step 6: Restart & Test

```bash
pm2 restart dropshipper-payout-app
```

Send a test email - it should have the "Dropshipper" label! 🎉

---

## 📋 Current .env Configuration

Your `.env` file now has:

```env
# SMTP (Working - emails send)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_SECURE=false
SMTP_USER=shubhankarhaldar07@gmail.com
SMTP_PASSWORD=qzzhrswnidzswfvz

# Gmail API (Needs setup - for labels)
GMAIL_CLIENT_ID=your_client_id_here
GMAIL_CLIENT_SECRET=your_client_secret_here
GMAIL_REDIRECT_URI=urn:ietf:wg:oauth:2.0:oob
GMAIL_REFRESH_TOKEN=your_refresh_token_here
```

---

## 🔍 Check Setup Status

Run the helper script:

```bash
./setup-gmail-oauth.sh
```

---

## 📚 Full Guide

For detailed step-by-step instructions, see: **GMAIL_API_SETUP.md**

---

## ⚠️ Important Notes

- **SMTP is working now** - emails are being sent
- **Labels require Gmail API OAuth2** - follow the steps above
- Once OAuth2 is configured, labels will be applied automatically
- The "Dropshipper" label will be created automatically if it doesn't exist
