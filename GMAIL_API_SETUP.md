# Gmail API OAuth2 Setup Guide - Step by Step

This guide will help you set up Gmail API OAuth2 to automatically apply the "Dropshipper" label to all sent emails.

## Prerequisites
- A Google account (shubhankarhaldar07@gmail.com)
- Access to Google Cloud Console

---

## Step 1: Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click on the project dropdown at the top
3. Click **"New Project"**
4. Enter project name: `Dropshipper Payout System` (or any name you prefer)
5. Click **"Create"**
6. Wait for the project to be created and select it

---

## Step 2: Enable Gmail API

1. In the Google Cloud Console, go to **"APIs & Services"** → **"Library"**
2. Search for **"Gmail API"**
3. Click on **"Gmail API"** from the results
4. Click **"Enable"** button
5. Wait for the API to be enabled (this may take a few seconds)

---

## Step 3: Create OAuth 2.0 Credentials

1. Go to **"APIs & Services"** → **"Credentials"**
2. Click **"+ CREATE CREDENTIALS"** at the top
3. Select **"OAuth client ID"**

### If you see "Configure consent screen" first:
1. Click **"Configure consent screen"**
2. Select **"External"** (unless you have a Google Workspace account)
3. Click **"Create"**
4. Fill in the required fields:
   - **App name**: `Dropshipper Payout System`
   - **User support email**: `shubhankarhaldar07@gmail.com`
   - **Developer contact information**: `shubhankarhaldar07@gmail.com`
5. Click **"Save and Continue"**
6. On "Scopes" page, click **"Save and Continue"** (no need to add scopes here)
7. On "Test users" page, click **"Save and Continue"**
8. On "Summary" page, click **"Back to Dashboard"**

### Now create OAuth Client ID:
1. Go back to **"APIs & Services"** → **"Credentials"**
2. Click **"+ CREATE CREDENTIALS"** → **"OAuth client ID"**
3. Select **"Desktop app"** as the application type
4. Enter a name: `Dropshipper Email Client`
5. Click **"Create"**
6. **IMPORTANT**: A popup will appear with your credentials:
   - **Client ID**: Copy this value
   - **Client Secret**: Copy this value
   - **Save these values securely!**

---

## Step 4: Add Credentials to .env File

1. Open your `.env` file in the project root
2. Add the following lines (replace with your actual values):

```env
GMAIL_CLIENT_ID=paste_your_client_id_here
GMAIL_CLIENT_SECRET=paste_your_client_secret_here
GMAIL_REDIRECT_URI=urn:ietf:wg:oauth:2.0:oob
```

3. Save the file

---

## Step 5: Get Authorization URL

1. **Restart your server** to load the new environment variables:
   ```bash
   pm2 restart dropshipper-payout-app
   # or if running in dev mode, restart your dev server
   ```

2. Open your browser and visit:
   ```
   http://localhost:3007/api/gmail-oauth-setup
   ```
   (Replace `3007` with your actual port if different)

3. You should see a JSON response with an `authUrl`. Copy the entire `authUrl` value.

---

## Step 6: Authorize the Application

1. **Open the `authUrl` in your browser** (paste it in a new tab)
2. You'll see a Google sign-in page
3. Sign in with: `shubhankarhaldar07@gmail.com`
4. You may see a warning: **"Google hasn't verified this app"**
   - Click **"Advanced"**
   - Click **"Go to Dropshipper Payout System (unsafe)"**
5. You'll see a permissions screen asking to:
   - View and manage your mail
   - Manage your mail labels
6. Click **"Allow"**
7. **IMPORTANT**: You'll be redirected to a page showing:
   ```
   Please copy this code, switch to your application and paste it there:
   [AUTHORIZATION_CODE]
   ```
8. **Copy the entire authorization code** (it's a long string)

---

## Step 7: Get Refresh Token

1. Open a new terminal or use Postman/curl
2. Make a POST request to:
   ```
   http://localhost:3007/api/gmail-oauth-callback
   ```
   With the following JSON body:
   ```json
   {
     "code": "paste_your_authorization_code_here"
   }
   ```

   **Using curl:**
   ```bash
   curl -X POST http://localhost:3007/api/gmail-oauth-callback \
     -H "Content-Type: application/json" \
     -d '{"code":"YOUR_AUTHORIZATION_CODE_HERE"}'
   ```

   **Using Postman:**
   - Method: POST
   - URL: `http://localhost:3007/api/gmail-oauth-callback`
   - Headers: `Content-Type: application/json`
   - Body (raw JSON):
     ```json
     {
       "code": "YOUR_AUTHORIZATION_CODE_HERE"
     }
     ```

3. You'll receive a JSON response with a `refreshToken`
4. **Copy the `refreshToken` value**

---

## Step 8: Add Refresh Token to .env

1. Open your `.env` file
2. Add or update this line:
   ```env
   GMAIL_REFRESH_TOKEN=paste_your_refresh_token_here
   ```
3. Save the file

---

## Step 9: Restart Server and Test

1. **Restart your server:**
   ```bash
   pm2 restart dropshipper-payout-app
   ```

2. **Test sending an email:**
   - Go to your payout dashboard
   - Select a dropshipper and calculate payouts
   - Click "Send Mail"
   - Send a test email

3. **Verify the label:**
   - Go to Gmail (shubhankarhaldar07@gmail.com)
   - Check the "Sent" folder
   - The email should have the **"Dropshipper"** label applied automatically

---

## Troubleshooting

### Issue: "Gmail API OAuth2 not configured"
- **Solution**: Make sure all Gmail API variables are in your `.env` file and the server is restarted

### Issue: "Invalid grant" error
- **Solution**: The authorization code expires quickly. Get a new code and try again immediately

### Issue: "Access denied" or "Redirect URI mismatch"
- **Solution**: Make sure `GMAIL_REDIRECT_URI=urn:ietf:wg:oauth:2.0:oob` in your `.env` file

### Issue: Label not being applied
- **Solution**: 
  1. Check server logs: `pm2 logs dropshipper-payout-app`
  2. Verify the refresh token is correct
  3. Make sure Gmail API is enabled in Google Cloud Console

### Issue: "OAuth2 client not found"
- **Solution**: Double-check your Client ID and Client Secret in the `.env` file

---

## Security Notes

⚠️ **Important Security Reminders:**
- Never commit your `.env` file to Git
- Keep your Client Secret and Refresh Token secure
- The `.env` file is already in `.gitignore` - don't remove it

---

## Quick Reference

Your `.env` file should have these Gmail API variables:
```env
GMAIL_CLIENT_ID=your_client_id
GMAIL_CLIENT_SECRET=your_client_secret
GMAIL_REDIRECT_URI=urn:ietf:wg:oauth:2.0:oob
GMAIL_REFRESH_TOKEN=your_refresh_token
```

---

## Need Help?

If you encounter any issues:
1. Check the server logs: `pm2 logs dropshipper-payout-app --lines 50`
2. Verify all environment variables are set correctly
3. Make sure Gmail API is enabled in Google Cloud Console
4. Try getting a new authorization code if the old one expired

---

**Once setup is complete, all emails sent through the payout system will automatically have the "Dropshipper" label applied!** 🎉

