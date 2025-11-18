import mysql from 'mysql2/promise';
import { drizzle } from 'drizzle-orm/mysql2';
import * as schema from '@shared/schema';

// Parse connection details from env vars or DATABASE_URL
let connectionConfig: mysql.PoolOptions;

if (process.env.DATABASE_URL) {
  // Parse DATABASE_URL if provided
  const url = new URL(process.env.DATABASE_URL);
  connectionConfig = {
    host: url.hostname,
    port: parseInt(url.port || '3306', 10),
    user: url.username,
    password: url.password,
    database: url.pathname.slice(1), // Remove leading '/'
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
  };
} else if (process.env.DB_HOST && process.env.DB_USER && process.env.DB_PASSWORD && process.env.DB_NAME) {
  // Use individual env vars
  connectionConfig = {
    host: process.env.DB_HOST,
    port: parseInt(process.env.DB_PORT || '3306', 10),
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
  };
} else {
  throw new Error(
    'DATABASE_URL or DB_HOST, DB_USER, DB_PASSWORD, DB_NAME must be set. Did you forget to provision a database?',
  );
}

// Create MySQL connection pool with optimized settings for large uploads
export const pool = mysql.createPool({
  ...connectionConfig,
  waitForConnections: true,
  connectionLimit: 20, // Increased from 10 for better concurrency
  queueLimit: 0,
  acquireTimeout: 60000, // 60 seconds to acquire connection
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
  // Note: timeout is handled per-query, not at pool level
});
export const db = drizzle(pool, { schema, mode: 'default' });
