module.exports = {
  apps: [
    {
      name: 'dropshipper-payout-app',
      script: 'dist/index.js',
      instances: 'max', // Use all available CPU cores
      exec_mode: 'cluster',
      env: {
        NODE_ENV: 'production',
        PORT: 5000
      },
      env_production: {
        NODE_ENV: 'production',
        PORT: 5000
      },
      // Logging
      log_file: './logs/combined.log',
      out_file: './logs/out.log',
      error_file: './logs/error.log',
      log_date_format: 'YYYY-MM-DD HH:mm Z',
      
      // Process management
      max_memory_restart: '1G',
      min_uptime: '10s',
      max_restarts: 10,
      autorestart: true,
      
      // Health monitoring
      watch: false, // Set to true in development if needed
      ignore_watch: ['node_modules', 'logs', 'dist'],
      
      // Advanced settings
      kill_timeout: 5000,
      listen_timeout: 8000,
      wait_ready: false,
      
      // Environment variables
      env_file: '.env' // Optional: if you have .env file
    }
  ],
  
  // Deployment configuration (optional)
  deploy: {
    production: {
      user: 'node',
      host: 'YOUR_SERVER_IP',
      ref: 'origin/main',
      repo: 'YOUR_REPOSITORY_URL',
      path: '/var/www/dropshipper-payout-app',
      'pre-deploy-local': '',
      'post-deploy': 'npm install && npm run build && pm2 reload ecosystem.config.cjs --env production',
      'pre-setup': ''
    }
  }
};