# Gmail Refresh Token - Is It Mandatory?

## Short Answer: **YES, it's mandatory for Gmail API**

## Why Refresh Token is Required

### For Gmail API to Work:
- ✅ **Client ID** - Identifies your application
- ✅ **Client Secret** - Authenticates your application  
- ✅ **Refresh Token** - **REQUIRED** - Allows server to get access tokens automatically

### Without Refresh Token:
- ❌ Gmail API cannot authenticate
- ❌ System falls back to SMTP
- ❌ Emails send successfully ✅
- ❌ **Labels cannot be applied** ❌

---

## How It Works

### OAuth2 Flow:
1. **Client ID + Secret** → Get authorization URL
2. **User authorizes** → Get authorization code
3. **Authorization code** → Exchange for **Refresh Token** (one-time)
4. **Refresh Token** → Get Access Tokens (automatically, as needed)
5. **Access Token** → Use Gmail API (send emails, apply labels)

### Why Refresh Token?
- Access tokens expire quickly (usually 1 hour)
- Refresh token doesn't expire (unless revoked)
- Server can automatically get new access tokens using refresh token
- No user interaction needed after initial setup

---

## Current Status

Looking at your `.env` file:
```env
GMAIL_CLIENT_ID=your_client_id_here ✅
GMAIL_CLIENT_SECRET=your_client_secret_here ✅
GMAIL_REDIRECT_URI=urn:ietf:wg:oauth:2.0:oob ✅
GMAIL_REFRESH_TOKEN=your_refresh_token_here ❌ (Still placeholder)
```

**You have Client ID and Secret, but need the Refresh Token!**

---

## How to Get Refresh Token

### Quick Steps:

1. **Make sure Client ID and Secret are in `.env`** ✅ (You have this)

2. **Restart server:**
   ```bash
   pm2 restart dropshipper-payout-app
   ```

3. **Get authorization URL:**
   Visit: `http://localhost:3007/api/gmail-oauth-setup`
   Copy the `authUrl`

4. **Authorize:**
   - Open `authUrl` in browser
   - Sign in with `shubhankarhaldar07@gmail.com`
   - Click "Allow"
   - Copy the authorization code

5. **Get refresh token:**
   ```bash
   curl -X POST http://localhost:3007/api/gmail-oauth-callback \
     -H "Content-Type: application/json" \
     -d '{"code":"YOUR_AUTHORIZATION_CODE_HERE"}'
   ```

6. **Add to `.env`:**
   ```env
   GMAIL_REFRESH_TOKEN=the_actual_refresh_token_from_step_5
   ```

7. **Restart server:**
   ```bash
   pm2 restart dropshipper-payout-app
   ```

---

## What Happens Without Refresh Token?

### Current Behavior (Without Refresh Token):
```
User sends email
  → System checks: Client ID? ✅ Client Secret? ✅ Refresh Token? ❌
  → Falls back to SMTP
  → Email sent successfully ✅
  → Label NOT applied ❌ (SMTP limitation)
```

### With Refresh Token:
```
User sends email
  → System checks: Client ID? ✅ Client Secret? ✅ Refresh Token? ✅
  → Uses Gmail API
  → Gets/creates "Dropshipper" label
  → Sends email with label
  → Email sent successfully ✅
  → Label applied ✅
```

---

## Summary

| Component | Required? | Purpose |
|-----------|-----------|---------|
| Client ID | ✅ Yes | Identifies your app |
| Client Secret | ✅ Yes | Authenticates your app |
| **Refresh Token** | ✅ **Yes** | **Gets access tokens for Gmail API** |
| Redirect URI | ✅ Yes | OAuth2 redirect endpoint |

**Without refresh token = No Gmail API = No labels**

---

## Alternative: Use SMTP Only

If you don't want to set up refresh token:
- ✅ Emails will still send via SMTP
- ❌ Labels will NOT be applied
- ✅ No additional setup needed

But to get labels, you **must** complete the OAuth2 flow to get the refresh token.

---

**Bottom Line:** Refresh token is **mandatory** for Gmail API (labels). It's a one-time setup that takes about 5 minutes. Follow `GMAIL_API_SETUP.md` for detailed steps.

