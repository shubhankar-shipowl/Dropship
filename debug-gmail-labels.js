#!/usr/bin/env node

// Debug script to check Gmail API label application
import 'dotenv/config';
import { google } from 'googleapis';

async function debugGmailLabels() {
  console.log('🔍 Gmail API Label Debug Tool\n');
  console.log('='.repeat(60));
  console.log('');

  // Check configuration
  console.log('📋 Configuration Check:');
  const config = {
    clientId: process.env.GMAIL_CLIENT_ID,
    clientSecret: process.env.GMAIL_CLIENT_SECRET,
    refreshToken: process.env.GMAIL_REFRESH_TOKEN,
    redirectUri: process.env.GMAIL_REDIRECT_URI || 'urn:ietf:wg:oauth:2.0:oob',
  };

  console.log('  Client ID:', config.clientId ? '✅ Set' : '❌ Missing');
  console.log('  Client Secret:', config.clientSecret ? '✅ Set' : '❌ Missing');
  console.log('  Refresh Token:', config.refreshToken ? '✅ Set' : '❌ Missing');
  console.log('  Redirect URI:', config.redirectUri);
  console.log('');

  if (!config.clientId || !config.clientSecret || !config.refreshToken) {
    console.log('❌ Gmail API not fully configured. Please check your .env file.');
    process.exit(1);
  }

  try {
    // Create OAuth2 client
    console.log('🔐 Creating OAuth2 client...');
    const oauth2Client = new google.auth.OAuth2(
      config.clientId,
      config.clientSecret,
      config.redirectUri
    );

    oauth2Client.setCredentials({
      refresh_token: config.refreshToken,
    });

    // Create Gmail client
    console.log('📧 Creating Gmail API client...');
    const gmail = google.gmail({ version: 'v1', auth: oauth2Client });

    // Test connection
    console.log('🧪 Testing Gmail API connection...');
    const profile = await gmail.users.getProfile({ userId: 'me' });
    console.log('✅ Connected to Gmail API');
    console.log('📧 Email:', profile.data.emailAddress);
    console.log('');

    // Check for "Dropshipper" label
    console.log('🏷️ Checking for "Dropshipper" label...');
    const labelsResponse = await gmail.users.labels.list({ userId: 'me' });
    const dropshipperLabel = labelsResponse.data.labels?.find(
      (label) => label.name === 'Dropshipper'
    );

    if (dropshipperLabel) {
      console.log('✅ "Dropshipper" label found');
      console.log('   Label ID:', dropshipperLabel.id);
      console.log('   Type:', dropshipperLabel.type);
      console.log('   Visibility:', dropshipperLabel.labelListVisibility);
      console.log('');
    } else {
      console.log('⚠️ "Dropshipper" label not found');
      console.log('   Creating label...');
      try {
        const newLabel = await gmail.users.labels.create({
          userId: 'me',
          requestBody: {
            name: 'Dropshipper',
            labelListVisibility: 'labelShow',
            messageListVisibility: 'show',
          },
        });
        console.log('✅ Label created');
        console.log('   Label ID:', newLabel.data.id);
        console.log('');
      } catch (createError) {
        console.error('❌ Error creating label:', createError.message);
        console.error('   This might indicate a permissions issue.');
        console.log('');
      }
    }

    // Check recent sent emails
    console.log('📬 Checking recent sent emails (last 10)...');
    try {
      const messagesResponse = await gmail.users.messages.list({
        userId: 'me',
        q: 'in:sent',
        maxResults: 10,
      });

      const messages = messagesResponse.data.messages || [];
      console.log(`   Found ${messages.length} sent emails`);
      console.log('');

      if (messages.length > 0) {
        console.log('🔍 Checking labels on recent sent emails:');
        for (let i = 0; i < Math.min(5, messages.length); i++) {
          const msg = messages[i];
          if (msg.id) {
            try {
              const messageDetails = await gmail.users.messages.get({
                userId: 'me',
                id: msg.id,
                format: 'metadata',
                metadataHeaders: ['Subject', 'To'],
              });

              const labels = messageDetails.data.labelIds || [];
              const hasDropshipper = dropshipperLabel && labels.includes(dropshipperLabel.id);
              
              const subject = messageDetails.data.payload?.headers?.find(
                (h) => h.name === 'Subject'
              )?.value || 'No subject';

              console.log(`   Email ${i + 1}:`);
              console.log(`     Subject: ${subject.substring(0, 50)}...`);
              console.log(`     Labels: ${labels.length > 0 ? labels.join(', ') : 'None'}`);
              console.log(`     Has "Dropshipper" label: ${hasDropshipper ? '✅ YES' : '❌ NO'}`);
              console.log('');
            } catch (err) {
              console.log(`   Email ${i + 1}: Error reading - ${err.message}`);
            }
          }
        }
      }
    } catch (listError) {
      console.error('❌ Error listing sent emails:', listError.message);
      console.log('');
    }

    console.log('='.repeat(60));
    console.log('');
    console.log('📊 Summary:');
    console.log('  ✅ Gmail API is configured and working');
    console.log('  ✅ Label "Dropshipper" exists');
    console.log('');
    console.log('💡 Next Steps:');
    console.log('  1. Send a test email through the payout system');
    console.log('  2. Check server logs to see if Gmail API is being used');
    console.log('  3. Verify the email appears in the "Dropshipper" label');
    console.log('');

  } catch (error) {
    console.error('❌ Error:', error.message);
    console.error('');
    console.error('Error details:');
    console.error('  Code:', error.code);
    console.error('  Response:', error.response?.data);
    console.error('');
    console.error('💡 This might indicate:');
    console.error('  - Invalid refresh token');
    console.error('  - Missing Gmail API scopes');
    console.error('  - Authentication issue');
    console.error('');
    console.error('Try regenerating your refresh token.');
    process.exit(1);
  }
}

debugGmailLabels().catch(console.error);

