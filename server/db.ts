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
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
  // Note: timeout is handled per-query, not at pool level
});
export const db = drizzle(pool, { schema, mode: 'default' });

// Connection lifecycle management
let isConnected = false;
let isShuttingDown = false;

/**
 * Initialize and verify database connection
 * Call this when the app starts
 */
export async function initializeDatabase(): Promise<void> {
  if (isConnected) {
    console.log('✅ Database connection already initialized');
    return;
  }

  try {
    console.log('🔌 Initializing database connection...');
    const connection = await pool.getConnection();
    
    // Test the connection with a simple query
    await connection.query('SELECT 1');
    connection.release();
    
    isConnected = true;
    console.log('✅ Database connection pool initialized successfully');
    // Log connection pool info (pool is created with connectionLimit: 20)
    console.log(`📊 Connection pool initialized with connection limit: 20`);
  } catch (error: any) {
    console.error('❌ Failed to initialize database connection:', error.message);
    throw new Error(`Database connection failed: ${error.message}`);
  }
}

/**
 * Gracefully close database connection pool
 * Call this when the app is shutting down
 */
export async function closeDatabase(): Promise<void> {
  if (isShuttingDown) {
    console.log('⚠️ Database shutdown already in progress');
    return;
  }

  if (!isConnected) {
    console.log('ℹ️ Database connection not initialized, nothing to close');
    return;
  }

  isShuttingDown = true;
  console.log('🔌 Closing database connection pool...');

  try {
    await pool.end();
    isConnected = false;
    console.log('✅ Database connection pool closed successfully');
  } catch (error: any) {
    console.error('❌ Error closing database connection pool:', error.message);
    throw error;
  }
}

/**
 * Get connection pool status
 */
export function getDatabaseStatus(): {
  connected: boolean;
  shuttingDown: boolean;
  poolSize: number;
  freeConnections: number;
} {
  try {
    // Try to get pool internals (mysql2 pool structure)
    const poolInternal = (pool as any).pool;
    return {
      connected: isConnected,
      shuttingDown: isShuttingDown,
      poolSize: poolInternal?._allConnections?.length || 0,
      freeConnections: poolInternal?._freeConnections?.length || 0,
    };
  } catch (error) {
    // If we can't access pool internals, return basic status
    return {
      connected: isConnected,
      shuttingDown: isShuttingDown,
      poolSize: 0,
      freeConnections: 0,
    };
  }
}
