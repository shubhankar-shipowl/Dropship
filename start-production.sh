#!/bin/bash

# Production deployment script for Dropshipper Payout Application

echo "🚀 Starting production deployment..."

# Create logs directory if it doesn't exist
mkdir -p logs

# Set production environment
export NODE_ENV=production

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

# Start with PM2
echo "🔄 Starting application with PM2..."
pm2 start ecosystem.config.cjs --env production

# Show PM2 status
echo "📊 PM2 Status:"
pm2 list

# Show logs
echo "📝 Recent logs:"
pm2 logs dropshipper-payout-app --lines 10

echo "🎉 Production deployment completed!"
echo "💡 Use 'pm2 monit' to monitor the application"
echo "💡 Use 'pm2 logs dropshipper-payout-app' to view logs"
echo "💡 Use 'pm2 restart dropshipper-payout-app' to restart"