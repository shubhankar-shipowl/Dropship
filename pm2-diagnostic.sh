#!/bin/bash

# PM2 Diagnostic Script for Dropshipper Payout Application

echo "🔍 PM2 Diagnostic Report"
echo "======================="

# Check if PM2 is installed
echo "📦 PM2 Installation:"
if command -v pm2 &> /dev/null; then
    echo "✅ PM2 is installed: $(pm2 -v)"
else
    echo "❌ PM2 is not installed. Install with: npm install -g pm2"
    exit 1
fi

echo ""

# Check PM2 processes
echo "🏃 PM2 Process Status:"
pm2 list

echo ""

# Check specific app status
echo "📊 App Status:"
if pm2 show dropshipper-payout-app &> /dev/null; then
    pm2 show dropshipper-payout-app
else
    echo "❌ dropshipper-payout-app is not running"
fi

echo ""

# Check system resources
echo "💻 System Resources:"
echo "Memory usage: $(free -h | grep Mem | awk '{print $3"/"$2}')"
echo "Disk usage: $(df -h . | tail -1 | awk '{print $3"/"$2" ("$5" used)"}')"
echo "CPU load: $(uptime | cut -d',' -f3-)"

echo ""

# Check required directories
echo "📁 Directory Status:"
for dir in logs temp uploads dist; do
    if [ -d "$dir" ]; then
        echo "✅ $dir/ exists ($(ls -la $dir | wc -l) items)"
    else
        echo "❌ $dir/ missing - creating..."
        mkdir -p "$dir"
        chmod 755 "$dir"
    fi
done

echo ""

# Check environment file
echo "🔧 Environment Configuration:"
if [ -f ".env" ]; then
    echo "✅ .env file exists"
    echo "Environment variables loaded:"
    grep -E "^[A-Z_]+=" .env | cut -d'=' -f1 | sed 's/^/  - /'
else
    echo "❌ .env file missing"
fi

echo ""

# Check Node.js and dependencies
echo "📚 Dependencies:"
echo "Node.js: $(node -v)"
echo "NPM: $(npm -v)"

if [ -f "dist/index.js" ]; then
    echo "✅ Built application exists (dist/index.js)"
else
    echo "❌ Built application missing - run: npm run build"
fi

echo ""

# Check recent errors
echo "🚨 Recent PM2 Errors (last 20 lines):"
if pm2 logs dropshipper-payout-app --err --lines 20 2>/dev/null | grep -q "error\|Error\|ERROR"; then
    pm2 logs dropshipper-payout-app --err --lines 10 2>/dev/null | tail -10
else
    echo "✅ No recent errors found"
fi

echo ""

# Quick fixes recommendations
echo "🛠️  Quick Fixes:"
echo "1. Restart app: pm2 restart dropshipper-payout-app"
echo "2. Reload app: pm2 reload dropshipper-payout-app"  
echo "3. View logs: pm2 logs dropshipper-payout-app"
echo "4. Monitor: pm2 monit"
echo "5. Full restart: ./start-production.sh"

echo ""

# Port check
echo "🌐 Network Status:"
if netstat -tulpn 2>/dev/null | grep -q ":5000"; then
    echo "✅ Port 5000 is in use (application likely running)"
else
    echo "❌ Port 5000 is not in use (application not listening)"
fi

echo ""
echo "Diagnostic completed! 🎯"