# Troubleshooting Gmail Labels Not Being Applied

## Issue: Labels configured but not being applied

If you've configured Gmail API OAuth2 but labels are still not being applied, follow these steps:

## Step 1: Test Gmail API Connection

Visit this endpoint to test if Gmail API is working:
```
http://localhost:3007/api/test-gmail-api
```

**Expected Response (Success):**
```json
{
  "success": true,
  "message": "Gmail API is working correctly!",
  "profile": {
    "email": "your-email@gmail.com"
  },
  "label": {
    "name": "Dropshipper",
    "id": "Label_12345"
  }
}
```

**If you get an error:**
- Check the error message
- Verify your refresh token is correct
- Make sure the server was restarted after adding the refresh token

## Step 2: Check Server Logs

When sending an email, check the server logs:
```bash
pm2 logs dropshipper-payout-app --lines 50
```

**Look for these messages:**
- `📧 Using Gmail API to send email with label...` ✅ (Gmail API is being used)
- `📧 Using SMTP to send email...` ❌ (Falling back to SMTP - labels won't work)

**If you see Gmail API errors:**
- Note the error message
- Common errors:
  - `401 Unauthorized` - Refresh token is invalid or expired
  - `403 Forbidden` - Missing scopes or insufficient permissions
  - `Invalid grant` - Refresh token needs to be regenerated

## Step 3: Verify Configuration

Run the diagnostic:
```bash
node check-email-setup.js
```

**Check:**
- ✅ All Gmail API variables are set (not placeholders)
- ✅ Refresh token is a long string (not "your_refresh_token_here")
- ✅ Client ID and Secret are correct

## Step 4: Common Issues & Solutions

### Issue 1: "Gmail API error, falling back to SMTP"
**Cause:** Gmail API authentication failed
**Solution:**
1. Check server logs for the specific error
2. Verify refresh token is correct
3. Try regenerating the refresh token:
   - Visit: `http://localhost:3007/api/gmail-oauth-setup`
   - Get new authorization code
   - Exchange for new refresh token
   - Update `.env` and restart server

### Issue 2: "Invalid grant" error
**Cause:** Refresh token expired or was revoked
**Solution:**
1. Get a new refresh token (see Issue 1)
2. Make sure you're using the correct Google account
3. Check if 2-Step Verification is still enabled

### Issue 3: Gmail API works but labels not applied
**Cause:** Email is being sent via SMTP instead of Gmail API
**Solution:**
1. Check logs - if you see "Using SMTP", Gmail API failed
2. Check the error message in logs
3. Verify Gmail API test endpoint works

### Issue 4: Server not restarted
**Cause:** Environment variables not loaded
**Solution:**
```bash
pm2 restart dropshipper-payout-app
```

## Step 5: Verify Label Application

After sending an email:
1. Go to Gmail (the sender account)
2. Check "Sent" folder
3. Look for the email you just sent
4. Verify it has the "Dropshipper" label

**If label is missing:**
- Check server logs for Gmail API errors
- Verify Gmail API was used (not SMTP)
- Test Gmail API connection first

## Quick Debug Checklist

- [ ] Gmail API credentials in `.env` (not placeholders)
- [ ] Server restarted after adding credentials
- [ ] Test endpoint `/api/test-gmail-api` returns success
- [ ] Server logs show "Using Gmail API" (not "Using SMTP")
- [ ] No authentication errors in logs
- [ ] Label exists in Gmail (check via test endpoint)

## Still Not Working?

1. **Check the exact error in server logs:**
   ```bash
   pm2 logs dropshipper-payout-app --lines 100 | grep -i "gmail\|error\|label"
   ```

2. **Test Gmail API directly:**
   ```bash
   curl http://localhost:3007/api/test-gmail-api
   ```

3. **Verify refresh token:**
   - Make sure it's the full token (long string)
   - No extra spaces or quotes
   - Matches the token from OAuth2 callback

4. **Check Gmail account:**
   - Make sure you're checking the correct Gmail account
   - The label should appear in the sender's Gmail (shubhankarhaldar07@gmail.com)
   - Check "Sent" folder, not "Inbox"

---

**Remember:** Labels can ONLY be applied via Gmail API. If emails are sent via SMTP, labels will NOT be applied, even if Gmail API is configured.

