#!/usr/bin/env node

// Quick diagnostic script to check email setup status
import 'dotenv/config';

console.log('🔍 Email Setup Diagnostic\n');
console.log('='.repeat(50));
console.log('');

// Check SMTP Configuration
console.log('📧 SMTP Configuration:');
const smtpConfig = {
  host: process.env.SMTP_HOST,
  port: process.env.SMTP_PORT,
  secure: process.env.SMTP_SECURE,
  user: process.env.SMTP_USER,
  password: process.env.SMTP_PASSWORD ? '***' + process.env.SMTP_PASSWORD.slice(-4) : 'NOT SET',
};

Object.entries(smtpConfig).forEach(([key, value]) => {
  const status = value ? '✅' : '❌';
  console.log(`  ${status} ${key}: ${value || 'NOT SET'}`);
});

console.log('');
console.log('📬 Gmail API OAuth2 Configuration:');
const gmailConfig = {
  clientId: process.env.GMAIL_CLIENT_ID,
  clientSecret: process.env.GMAIL_CLIENT_SECRET,
  redirectUri: process.env.GMAIL_REDIRECT_URI,
  refreshToken: process.env.GMAIL_REFRESH_TOKEN,
};

let gmailConfigured = true;
Object.entries(gmailConfig).forEach(([key, value]) => {
  const isSet = value && !value.includes('your_') && !value.includes('here');
  const status = isSet ? '✅' : '❌';
  if (!isSet) gmailConfigured = false;
  const displayValue = key === 'refreshToken' && value 
    ? '***' + value.slice(-10) 
    : (value || 'NOT SET');
  console.log(`  ${status} ${key}: ${displayValue}`);
});

console.log('');
console.log('='.repeat(50));
console.log('');

// Summary
console.log('📊 Summary:');
console.log('');

if (smtpConfig.host && smtpConfig.user && smtpConfig.password) {
  console.log('✅ SMTP is configured - Emails can be sent');
} else {
  console.log('❌ SMTP is NOT configured - Emails cannot be sent');
}

if (gmailConfigured) {
  console.log('✅ Gmail API is configured - Labels CAN be applied');
  console.log('   🎉 Your emails will have the "Dropshipper" label!');
} else {
  console.log('❌ Gmail API is NOT configured - Labels CANNOT be applied');
  console.log('');
  console.log('📖 To enable labels:');
  console.log('   1. Follow the guide: GMAIL_API_SETUP.md');
  console.log('   2. Set up OAuth2 credentials in Google Cloud Console');
  console.log('   3. Uncomment and fill in Gmail API variables in .env');
  console.log('   4. Get refresh token and add to .env');
  console.log('   5. Restart server');
}

console.log('');
console.log('💡 Important: Gmail labels can ONLY be applied via Gmail API, not SMTP.');
console.log('   This is a Gmail limitation - SMTP does not support label operations.');

