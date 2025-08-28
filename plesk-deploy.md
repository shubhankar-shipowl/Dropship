# Plesk Deployment Guide

## Prerequisites
- Node.js 18+ enabled in Plesk
- PostgreSQL database created in Plesk
- Domain/subdomain configured

## Step 1: Upload Files
Upload all project files to your domain's root directory (httpdocs/) or a subdirectory.

## Step 2: Install Dependencies
In Plesk Node.js settings or via SSH:
```bash
npm install --production
npm run build
```

## Step 3: Configure Environment Variables
In Plesk → Node.js → Environment Variables, add:

```
NODE_ENV=production
DATABASE_URL=postgresql://username:password@localhost:5432/database_name
SESSION_SECRET=your-secure-session-secret-here
PORT=3000
```

## Step 4: Configure Plesk Node.js Application
1. Go to Plesk → Node.js
2. Set **Application Startup File**: `app.js` or `server.js`
3. Set **Application Mode**: Production
4. Set **Node.js Version**: 18.x or higher

## Step 5: Database Setup
Run database migrations:
```bash
npm run db:push
```

## Step 6: Configure Reverse Proxy (if needed)
If using subdirectory deployment, configure Apache/Nginx reverse proxy:

**Apache (.htaccess already provided)**

**Nginx configuration:**
```nginx
location /api/ {
    proxy_pass http://localhost:3000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection 'upgrade';
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_cache_bypass $http_upgrade;
}
```

## Step 7: File Structure After Build
```
httpdocs/
├── dist/
│   ├── public/          # Frontend build files
│   └── index.js         # Backend build file
├── app.js               # Plesk entry point
├── server.js            # Alternative entry point
├── .htaccess            # Apache configuration
├── package.json         # Dependencies
└── node_modules/        # Installed packages
```

## Common Issues and Solutions

### Issue: App doesn't start
- Check Node.js logs in Plesk
- Verify environment variables are set
- Ensure DATABASE_URL is correct

### Issue: Static files not served
- Check if build process completed
- Verify .htaccess configuration
- Check file permissions (755 for directories, 644 for files)

### Issue: API requests fail
- Check if reverse proxy is configured
- Verify CORS settings in server code
- Check firewall/security settings

### Issue: Database connection fails
- Verify DATABASE_URL format
- Check PostgreSQL user permissions
- Ensure database exists

## Production Optimizations

1. **Enable Compression**: Already configured in .htaccess
2. **Set Security Headers**: Already configured in .htaccess
3. **Configure Caching**: Already configured in .htaccess
4. **SSL/HTTPS**: Configure in Plesk SSL/TLS settings
5. **Monitoring**: Use Plesk monitoring tools

## Maintenance

### Updating the Application
1. Upload new files
2. Run `npm install` if dependencies changed
3. Run `npm run build`
4. Restart Node.js application in Plesk

### Database Migrations
```bash
npm run db:push
```

### Logs
- Check Plesk → Node.js → Logs for application logs
- Check Apache/Nginx error logs for web server issues