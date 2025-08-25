module.exports = {
  apps: [
    {
      name: 'dropshipper-payout-app',
      script: 'dist/index.js',
      
      // Process Configuration - Optimized for file uploads
      instances: 1,
      exec_mode: 'fork',
      
      // Working Directory - Critical for file paths
      cwd: process.cwd(), // Ensures correct working directory
      
      // Environment Variables
      env: {
        NODE_ENV: 'production',
        PORT: 5000,
        // Ensure Node.js can handle large file uploads
        NODE_OPTIONS: '--max-old-space-size=2048'
      },
      env_production: {
        NODE_ENV: 'production',
        PORT: 5000,
        NODE_OPTIONS: '--max-old-space-size=2048'
      },
      
      // Environment File Loading
      env_file: '.env',
      
      // Logging Configuration
      log_file: './logs/combined.log',
      out_file: './logs/out.log',
      error_file: './logs/error.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      log_type: 'json',
      
      // Process Management - Enhanced
      max_memory_restart: '2G', // Increased for large file processing
      min_uptime: '30s', // Longer minimum uptime
      max_restarts: 15, // More restart attempts
      restart_delay: 4000, // Delay between restarts
      autorestart: true,
      
      // Health Monitoring
      watch: false,
      ignore_watch: [
        'node_modules',
        'logs',
        'dist',
        '.git',
        '*.log',
        'temp',
        'uploads'
      ],
      
      // Advanced Process Settings
      kill_timeout: 10000, // More time for graceful shutdown
      listen_timeout: 10000, // More time for app to start listening
      wait_ready: true, // Wait for ready signal
      
      // File Upload Optimizations
      source_map_support: false, // Disable for better performance
      disable_trace: true, // Disable tracing for better performance
      
      // Error Handling
      exp_backoff_restart_delay: 100,
      
      // Process Title for easier identification
      name: 'dropshipper-payout-app',
      
      // Additional PM2 Features
      post_update: ['npm install', 'npm run build'],
      
      // Node.js Process Arguments
      node_args: [
        '--max-old-space-size=2048',
        '--optimize-for-size'
      ],
      
      // Interpreter Arguments
      interpreter_args: '--harmony',
      
      // Error Handling for Production
      pmx: false, // Disable PMX for better performance
      
      // Instance Variables for debugging
      instance_var: 'NODE_APP_INSTANCE'
    }
  ],
  
  // Deployment configuration
  deploy: {
    production: {
      user: 'node',
      host: 'YOUR_SERVER_IP',
      ref: 'origin/main',
      repo: 'YOUR_REPOSITORY_URL',
      path: '/var/www/dropshipper-payout-app',
      'pre-deploy-local': '',
      'post-deploy': 'npm install && npm run build && pm2 reload ecosystem.config.cjs --env production && pm2 save',
      'pre-setup': 'mkdir -p logs temp uploads'
    }
  }
};