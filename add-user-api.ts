/**
 * Script to add a user via the API endpoint
 * Make sure your server is running before executing this script
 * 
 * Usage: npm run add-user-api
 * Or: tsx add-user-api.ts
 */

const username = 'finance@shipowl.io';
const email = 'finance@shipowl.io';
const password = 'Shipowl@6';

const API_URL = process.env.API_URL || 'http://localhost:5000';

async function addUserViaAPI() {
  try {
    console.log('📡 Adding user via API endpoint...');
    console.log(`API URL: ${API_URL}`);
    
    const response = await fetch(`${API_URL}/api/auth/create-user`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        username,
        email,
        password,
      }),
    });

    const data = await response.json();

    if (!response.ok) {
      console.error('❌ Error:', data.message || 'Failed to create user');
      process.exit(1);
    }

    console.log('✅ User created successfully!');
    console.log('Username:', data.user.username);
    console.log('Email:', data.user.email);
    console.log('ID:', data.user.id);
    
    process.exit(0);
  } catch (error: any) {
    console.error('❌ Error:', error.message || error);
    console.error('\n💡 Make sure your server is running on', API_URL);
    process.exit(1);
  }
}

addUserViaAPI();

