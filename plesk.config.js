/**
 * Plesk-specific configuration for deployment
 * This file contains settings optimized for Plesk hosting environments
 */

export const pleskConfig = {
  // Port configuration for Plesk
  port: process.env.PORT || 3000,
  
  // Database configuration for Plesk PostgreSQL
  database: {
    url: process.env.DATABASE_URL || 'postgresql://localhost:5432/your_db_name',
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
    pool: {
      min: 2,
      max: 10
    }
  },
  
  // Session configuration for production
  session: {
    secret: process.env.SESSION_SECRET || 'change-this-in-production',
    cookie: {
      secure: process.env.NODE_ENV === 'production', // HTTPS in production
      maxAge: 24 * 60 * 60 * 1000, // 24 hours
      httpOnly: true,
      sameSite: 'lax'
    }
  },
  
  // CORS configuration for production
  cors: {
    origin: process.env.ALLOWED_ORIGINS ? process.env.ALLOWED_ORIGINS.split(',') : true,
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'],
    allowedHeaders: ['Origin', 'X-Requested-With', 'Content-Type', 'Accept', 'Authorization']
  },
  
  // File upload limits
  upload: {
    maxFileSize: '200mb',
    allowedMimeTypes: [
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'application/vnd.ms-excel',
      'text/csv'
    ]
  },
  
  // Security headers for production
  security: {
    contentSecurityPolicy: false, // Disable if causing issues with React
    hsts: process.env.NODE_ENV === 'production',
    noSniff: true,
    xssFilter: true,
    referrerPolicy: 'same-origin'
  }
};

export default pleskConfig;