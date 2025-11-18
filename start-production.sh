#!/bin/bash

# Production deployment script for Dropshipper Payout Application

echo "🚀 Starting production deployment..."

# Create required directories
echo "📁 Creating required directories..."
mkdir -p logs temp uploads
chmod 755 logs temp uploads

# Set production environment
export NODE_ENV=production

# Clean previous processes
echo "🧹 Cleaning previous PM2 processes..."
pm2 delete dropshipper-payout-app 2>/dev/null || echo "No previous process to clean"

# Build the application
echo "📦 Building application..."
npm run build

# Check if build was successful
if [ $? -eq 0 ]; then
    echo "✅ Build completed successfully"
else
    echo "❌ Build failed"
    exit 1
fi

# Verify environment file exists
if [ ! -f .env ]; then
    echo "⚠️  Warning: .env file not found. Make sure to configure environment variables."
    echo "💡 Copy .env.example to .env and update with your settings"
fi

# Start with PM2
echo "🔄 Starting application with PM2..."
pm2 start ecosystem.config.cjs --env production

# Save PM2 configuration
echo "💾 Saving PM2 configuration..."
pm2 save

# Show PM2 status
echo "📊 PM2 Status:"
pm2 list

# Show recent logs
echo "📝 Recent logs:"
pm2 logs dropshipper-payout-app --lines 15

# Health check
echo "🏥 Performing health check..."
sleep 5
if pm2 show dropshipper-payout-app | grep -q "online"; then
    echo "✅ Application is running successfully!"
else
    echo "❌ Application failed to start properly"
    echo "🔍 Check logs with: pm2 logs dropshipper-payout-app"
    exit 1
fi

echo "🎉 Production deployment completed successfully!"
echo ""
echo "📋 Management Commands:"
echo "💡 Monitor: pm2 monit"
echo "💡 Logs: pm2 logs dropshipper-payout-app"
echo "💡 Restart: pm2 restart dropshipper-payout-app"
echo "💡 Stop: pm2 stop dropshipper-payout-app"
echo "💡 Status: pm2 status"
echo ""
echo "🌐 Application should be available on port 5000"