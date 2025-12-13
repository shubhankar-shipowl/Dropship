# Gmail Label Issue - Analysis & Fix

## 🔍 Problem Identified

**Issue:** Emails are being sent via SMTP instead of Gmail API, so labels are NOT being applied.

**Evidence from Debug:**
- ✅ Gmail API is configured correctly
- ✅ "Dropshipper" label exists (ID: Label_7525342047555694586)
- ❌ Recent sent emails don't have the "Dropshipper" label
- ❌ All recent emails only show "SENT" label (default Gmail label)

## 🎯 Root Cause

The code is **falling back to SMTP** even though Gmail API is configured. This happens when:
1. Gmail API authentication fails silently
2. Gmail API send operation fails
3. Error is caught and code falls back to SMTP

## ✅ Solution

### Step 1: Check Server Logs

When you send an email, check the server logs:
```bash
pm2 logs dropshipper-payout-app --lines 50
```

**Look for:**
- `📧 Using Gmail API to send email with label...` ✅ (Gmail API is being used)
- `📧 Using SMTP to send email...` ❌ (Falling back to SMTP - labels won't work)
- Any Gmail API error messages

### Step 2: Test Gmail API Directly

Run the debug script:
```bash
node debug-gmail-labels.js
```

This will show:
- If Gmail API connection works
- If label exists
- If recent emails have labels

### Step 3: Send a Test Email

1. Go to Payout Dashboard
2. Select a dropshipper
3. Calculate payouts
4. Click "Send Mail"
5. Send the email
6. **Immediately check server logs** to see which method was used

### Step 4: Verify Label Application

After sending, check Gmail:
1. Go to Gmail (shubhankar@shipowl.io)
2. Check "Sent" folder
3. Look for the email you just sent
4. Verify it has the "Dropshipper" label

## 🔧 Common Issues & Fixes

### Issue 1: "Gmail API error, falling back to SMTP"

**Possible causes:**
- Invalid refresh token
- Missing Gmail API scopes
- Authentication error

**Fix:**
1. Check server logs for the exact error
2. Regenerate refresh token if needed:
   - Visit: `http://localhost:3007/api/gmail-oauth-setup`
   - Get new authorization code
   - Exchange for new refresh token
   - Update `.env` and restart server

### Issue 2: Gmail API works but labels not applied

**Possible causes:**
- Email sent via SMTP (not Gmail API)
- Label ID mismatch
- Gmail API send failed silently

**Fix:**
1. Check logs to confirm Gmail API was used
2. Verify label ID is correct
3. Check for Gmail API errors in logs

### Issue 3: Server not restarted

**Fix:**
```bash
pm2 restart dropshipper-payout-app
```

## 📊 Expected Behavior

### When Gmail API is Used:
```
📧 Using Gmail API to send email with label...
🔍 Gmail API client initialized, attempting to get/create label...
✅ Label ID obtained: Label_7525342047555694586
📤 Sending email via Gmail API with label ID: Label_7525342047555694586
✅ Email sent via Gmail API
🏷️ Labels applied to email: [..., 'Label_7525342047555694586', 'SENT']
✅ "Dropshipper" label confirmed on sent email
```

### When SMTP is Used (Labels Won't Work):
```
📧 Using SMTP to send email...
🔍 Verifying SMTP connection...
✅ SMTP server is ready to send emails
✅ Email sent successfully!
```

## 🎯 Next Steps

1. **Restart server** to load latest code with enhanced logging
2. **Send a test email** and watch the logs
3. **Check which method is used** (Gmail API or SMTP)
4. **If SMTP is used**, check the error message in logs
5. **Fix the issue** based on the error message

## 💡 Key Points

- **Labels can ONLY be applied via Gmail API**
- **SMTP cannot apply labels** (Gmail limitation)
- **If you see "Using SMTP" in logs, labels won't work**
- **Check server logs immediately after sending email**

---

**The debug script shows Gmail API is working, so the issue is likely that emails are being sent via SMTP instead of Gmail API. Check the server logs to see why.**

