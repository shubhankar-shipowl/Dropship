#!/bin/bash

# Gmail API OAuth2 Setup Helper Script
# This script helps you set up Gmail API OAuth2 for applying labels

echo "🔧 Gmail API OAuth2 Setup Helper"
echo "=================================="
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found!"
    exit 1
fi

# Check if Gmail API credentials are set
if grep -q "GMAIL_CLIENT_ID=" .env && grep -q "GMAIL_CLIENT_SECRET=" .env; then
    CLIENT_ID=$(grep "GMAIL_CLIENT_ID=" .env | cut -d '=' -f2)
    CLIENT_SECRET=$(grep "GMAIL_CLIENT_SECRET=" .env | cut -d '=' -f2)
    
    if [ "$CLIENT_ID" != "your_client_id_here" ] && [ "$CLIENT_SECRET" != "your_client_secret_here" ]; then
        echo "✅ Gmail API credentials found in .env"
        echo ""
        echo "📋 Next steps:"
        echo "1. Make sure your server is running"
        echo "2. Visit: http://localhost:3007/api/gmail-oauth-setup"
        echo "3. Copy the authUrl and open it in your browser"
        echo "4. Authorize the application and copy the authorization code"
        echo "5. Run: curl -X POST http://localhost:3007/api/gmail-oauth-callback -H 'Content-Type: application/json' -d '{\"code\":\"YOUR_CODE_HERE\"}'"
        echo ""
        
        # Check if refresh token is set
        if grep -q "GMAIL_REFRESH_TOKEN=" .env; then
            REFRESH_TOKEN=$(grep "GMAIL_REFRESH_TOKEN=" .env | cut -d '=' -f2)
            if [ "$REFRESH_TOKEN" != "your_refresh_token_here" ] && [ ! -z "$REFRESH_TOKEN" ]; then
                echo "✅ Gmail API refresh token is already configured!"
                echo "🎉 Your Gmail API OAuth2 setup is complete!"
                echo ""
                echo "All emails sent through the payout system will now have the 'Dropshipper' label applied automatically."
            else
                echo "⚠️  Gmail API refresh token not set yet"
                echo "   Follow the steps above to get your refresh token"
            fi
        else
            echo "⚠️  Gmail API refresh token not set yet"
            echo "   Follow the steps above to get your refresh token"
        fi
    else
        echo "⚠️  Gmail API credentials not configured yet"
        echo ""
        echo "📖 Please follow the guide in GMAIL_API_SETUP.md to:"
        echo "   1. Create OAuth2 credentials in Google Cloud Console"
        echo "   2. Add GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET to .env"
        echo "   3. Run this script again"
    fi
else
    echo "⚠️  Gmail API credentials not found in .env"
    echo ""
    echo "📖 Please follow the guide in GMAIL_API_SETUP.md to set up Gmail API OAuth2"
fi

echo ""
echo "📚 For detailed instructions, see: GMAIL_API_SETUP.md"

