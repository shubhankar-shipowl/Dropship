# Why Labels Are Not Being Applied

## 🔍 Current Status

**Diagnostic Result:**
- ✅ **SMTP is configured** - Emails are being sent successfully
- ❌ **Gmail API is NOT configured** - Labels cannot be applied

## ❓ Why Labels Aren't Working

### The Problem:
**Gmail labels can ONLY be applied via Gmail API, NOT via SMTP.**

This is a fundamental limitation:
- **SMTP** (Simple Mail Transfer Protocol) - Can only send emails, cannot manage labels
- **Gmail API** - Can send emails AND apply labels, but requires OAuth2 authentication

### Current Flow:
1. System tries to use Gmail API first
2. ❌ Gmail API credentials are NOT configured (commented out in `.env`)
3. System falls back to SMTP
4. ✅ Email is sent successfully via SMTP
5. ❌ **Labels cannot be applied** (SMTP limitation)

---

## ✅ Solution: Set Up Gmail API OAuth2

To enable labels, you MUST set up Gmail API OAuth2. Here's what you need:

### Required Steps:

1. **Get OAuth2 Credentials from Google Cloud Console**
   - Follow: `GMAIL_API_SETUP.md` (detailed guide)
   - Or: `GMAIL_OAUTH2_QUICK_START.md` (quick reference)

2. **Update your `.env` file:**
   ```env
   # Uncomment and fill these in:
   GMAIL_CLIENT_ID=your_actual_client_id_here
   GMAIL_CLIENT_SECRET=your_actual_client_secret_here
   GMAIL_REDIRECT_URI=urn:ietf:wg:oauth:2.0:oob
   GMAIL_REFRESH_TOKEN=your_actual_refresh_token_here
   ```

3. **Restart your server:**
   ```bash
   pm2 restart dropshipper-payout-app
   ```

4. **Test:**
   - Send an email
   - Check Gmail Sent folder
   - Verify "Dropshipper" label is applied

---

## 🔧 Quick Check

Run the diagnostic script:
```bash
node check-email-setup.js
```

This will show you exactly what's configured and what's missing.

---

## 📋 What Happens After Setup

### Before (Current - SMTP Only):
```
User clicks "Send Mail" 
  → System checks Gmail API (not configured)
  → Falls back to SMTP
  → Email sent ✅
  → Label NOT applied ❌
```

### After (With Gmail API):
```
User clicks "Send Mail"
  → System checks Gmail API (configured ✅)
  → Gets/creates "Dropshipper" label
  → Sends email via Gmail API with label
  → Email sent ✅
  → Label applied ✅
```

---

## ⚠️ Important Notes

1. **SMTP will still work** - Even after setting up Gmail API, SMTP remains as a fallback
2. **Labels require Gmail API** - There's no way around this Gmail limitation
3. **OAuth2 is required** - Gmail API requires OAuth2 authentication for security
4. **One-time setup** - Once configured, it works automatically for all emails

---

## 🆘 Still Having Issues?

1. **Check your `.env` file:**
   ```bash
   cat .env | grep GMAIL
   ```
   Make sure values are NOT commented out and are actual values (not placeholders)

2. **Check server logs:**
   ```bash
   pm2 logs dropshipper-payout-app --lines 50
   ```
   Look for Gmail API related messages

3. **Verify OAuth2 setup:**
   - Visit: `http://localhost:3007/api/gmail-oauth-setup`
   - Should return an `authUrl` if credentials are set

4. **Test Gmail API connection:**
   - Try sending an email
   - Check logs for "Using Gmail API" message
   - If you see "Using SMTP", Gmail API is not configured

---

## 📚 Documentation

- **GMAIL_API_SETUP.md** - Complete step-by-step guide
- **GMAIL_OAUTH2_QUICK_START.md** - Quick 5-step guide
- **setup-gmail-oauth.sh** - Helper script to check status

---

**Bottom Line:** Labels require Gmail API OAuth2 setup. SMTP alone cannot apply labels. Follow the setup guide to enable labels! 🎯

