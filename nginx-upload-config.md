# Nginx Configuration for Large File Uploads

If you're using Nginx as a reverse proxy in front of your PM2 application, you need to configure it to handle large file uploads and long timeouts.

Add these settings to your Nginx configuration file (usually in `/etc/nginx/sites-available/your-site`):

```nginx
server {
    # ... other settings ...

    # Increase client body size limit for large file uploads
    client_max_body_size 250M;
    
    # Increase timeouts for large file processing
    proxy_connect_timeout 1800s;
    proxy_send_timeout 1800s;
    proxy_read_timeout 1800s;
    send_timeout 1800s;
    
    # Keep connections alive
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    
    # Buffer settings
    proxy_buffering off;
    proxy_request_buffering off;
    
    location /api/upload {
        # Specific settings for upload endpoint
        proxy_pass http://localhost:5000;
        proxy_connect_timeout 1800s;
        proxy_send_timeout 1800s;
        proxy_read_timeout 1800s;
        client_max_body_size 250M;
        proxy_buffering off;
        proxy_request_buffering off;
    }
    
    location / {
        proxy_pass http://localhost:5000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;
    }
}
```

After updating, reload Nginx:
```bash
sudo nginx -t  # Test configuration
sudo systemctl reload nginx  # Reload if test passes
```

## Apache Configuration

If using Apache, add to your `.htaccess` or virtual host config:

```apache
# Increase upload limits
LimitRequestBody 262144000  # 250MB in bytes
TimeOut 1800  # 30 minutes

# For specific upload endpoint
<Location "/api/upload">
    TimeOut 1800
    LimitRequestBody 262144000
</Location>
```

