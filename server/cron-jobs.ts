import cron from 'node-cron';
import { exec } from 'child_process';
import { promisify } from 'util';
import * as fs from 'fs/promises';
import * as path from 'path';
import { pool, getDatabaseStatus } from './db';

const execAsync = promisify(exec);

// Store cron task references for proper cleanup
const cronTasks: cron.ScheduledTask[] = [];

// Get database connection details from environment
function getDbConfig() {
  if (process.env.DATABASE_URL) {
    const url = new URL(process.env.DATABASE_URL);
    return {
      host: url.hostname,
      port: url.port || '3306',
      user: url.username,
      password: url.password,
      database: url.pathname.slice(1),
    };
  } else if (process.env.DB_HOST && process.env.DB_USER && process.env.DB_PASSWORD && process.env.DB_NAME) {
    return {
      host: process.env.DB_HOST,
      port: process.env.DB_PORT || '3306',
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
    };
  }
  return null;
}

/**
 * Create database backup using mysqldump
 */
async function createDatabaseBackup(): Promise<string | null> {
  const dbConfig = getDbConfig();
  if (!dbConfig) {
    console.error('❌ [BACKUP] Database configuration not found');
    return null;
  }

  try {
    // Create backups directory if it doesn't exist
    const backupsDir = path.join(process.cwd(), 'backups');
    await fs.mkdir(backupsDir, { recursive: true });

    // Generate backup filename with timestamp
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').split('T')[0];
    const time = new Date().toLocaleTimeString('en-IN', { 
      timeZone: 'Asia/Kolkata',
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    }).replace(/:/g, '-');
    const backupFilename = `backup_${dbConfig.database}_${timestamp}_${time}.sql`;
    const backupPath = path.join(backupsDir, backupFilename);

    // Build mysqldump command
    // Note: Password is passed via MYSQL_PWD environment variable for security
    const mysqldumpCmd = `mysqldump -h ${dbConfig.host} -P ${dbConfig.port} -u ${dbConfig.user} ${dbConfig.database} > ${backupPath}`;

    // Execute backup with password in environment variable
    await execAsync(mysqldumpCmd, {
      env: { ...process.env, MYSQL_PWD: dbConfig.password },
      maxBuffer: 10 * 1024 * 1024, // 10MB buffer
    });

    // Get file size
    const stats = await fs.stat(backupPath);
    const fileSizeMB = (stats.size / (1024 * 1024)).toFixed(2);

    console.log(`✅ [BACKUP] Database backup created successfully`);
    console.log(`📁 [BACKUP] File: ${backupPath}`);
    console.log(`📊 [BACKUP] Size: ${fileSizeMB} MB`);

    // Clean up old backups (keep only last 30 days)
    await cleanupOldBackups(backupsDir, 30);

    return backupPath;
  } catch (error: any) {
    console.error('❌ [BACKUP] Failed to create database backup:', error.message);
    // If mysqldump is not available, try alternative method using Node.js
    if (error.message.includes('mysqldump') || error.code === 'ENOENT') {
      console.log('⚠️ [BACKUP] mysqldump not found, trying alternative backup method...');
      return await createBackupAlternative(dbConfig);
    }
    return null;
  }
}

/**
 * Alternative backup method using Node.js (if mysqldump is not available)
 */
async function createBackupAlternative(dbConfig: any): Promise<string | null> {
  try {
    const backupsDir = path.join(process.cwd(), 'backups');
    await fs.mkdir(backupsDir, { recursive: true });

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').split('T')[0];
    const time = new Date().toLocaleTimeString('en-IN', { 
      timeZone: 'Asia/Kolkata',
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    }).replace(/:/g, '-');
    const backupFilename = `backup_${dbConfig.database}_${timestamp}_${time}.sql`;
    const backupPath = path.join(backupsDir, backupFilename);

    // Get all tables
    const [tables] = await pool.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
      [dbConfig.database]
    ) as any[];

    let backupContent = `-- Database Backup\n`;
    backupContent += `-- Database: ${dbConfig.database}\n`;
    backupContent += `-- Created: ${new Date().toISOString()}\n`;
    backupContent += `-- Timezone: Asia/Kolkata\n\n`;

    for (const table of tables) {
      const tableName = table.table_name;
      backupContent += `\n-- Table: ${tableName}\n`;
      
      // Get table structure
      const [createTable] = await pool.query(`SHOW CREATE TABLE \`${tableName}\``) as any[];
      if (createTable && createTable[0]) {
        backupContent += `DROP TABLE IF EXISTS \`${tableName}\`;\n`;
        backupContent += `${createTable[0]['Create Table']};\n\n`;
      }

      // Get table data
      const [rows] = await pool.query(`SELECT * FROM \`${tableName}\``) as any[];
      if (rows && rows.length > 0) {
        backupContent += `-- Data for table ${tableName}\n`;
        for (const row of rows) {
          const columns = Object.keys(row).map(k => `\`${k}\``).join(', ');
          const values = Object.values(row).map((v: any) => {
            if (v === null) return 'NULL';
            if (typeof v === 'string') return `'${v.replace(/'/g, "''")}'`;
            return v;
          }).join(', ');
          backupContent += `INSERT INTO \`${tableName}\` (${columns}) VALUES (${values});\n`;
        }
      }
    }

    await fs.writeFile(backupPath, backupContent, 'utf8');
    const stats = await fs.stat(backupPath);
    const fileSizeMB = (stats.size / (1024 * 1024)).toFixed(2);

    console.log(`✅ [BACKUP] Database backup created successfully (alternative method)`);
    console.log(`📁 [BACKUP] File: ${backupPath}`);
    console.log(`📊 [BACKUP] Size: ${fileSizeMB} MB`);

    await cleanupOldBackups(backupsDir, 30);
    return backupPath;
  } catch (error: any) {
    console.error('❌ [BACKUP] Alternative backup method also failed:', error.message);
    return null;
  }
}

/**
 * Clean up old backup files (keep only last N days)
 */
async function cleanupOldBackups(backupsDir: string, keepDays: number): Promise<void> {
  try {
    const files = await fs.readdir(backupsDir);
    const now = Date.now();
    const maxAge = keepDays * 24 * 60 * 60 * 1000; // Convert days to milliseconds

    let deletedCount = 0;
    for (const file of files) {
      if (!file.endsWith('.sql')) continue;

      const filePath = path.join(backupsDir, file);
      const stats = await fs.stat(filePath);
      const fileAge = now - stats.mtimeMs;

      if (fileAge > maxAge) {
        await fs.unlink(filePath);
        deletedCount++;
        console.log(`🗑️  [BACKUP] Deleted old backup: ${file}`);
      }
    }

    if (deletedCount > 0) {
      console.log(`🧹 [BACKUP] Cleaned up ${deletedCount} old backup file(s)`);
    }
  } catch (error: any) {
    console.error('⚠️ [BACKUP] Error cleaning up old backups:', error.message);
  }
}

/**
 * Initialize all cron jobs
 * Call this when the app starts
 */
export function initializeCronJobs(): void {
  console.log('⏰ Initializing cron jobs...');

  // Database connection health check - runs daily at 3 AM IST (Indian Standard Time)
  // IST is UTC+5:30, so 3 AM IST = 21:30 UTC (previous day)
  // Using timezone: 'Asia/Kolkata' for IST
  const dbHealthCheckTask = cron.schedule('0 3 * * *', async () => {
    console.log('⏰ [CRON] Running daily database connection health check at 3 AM IST...');
    
    try {
      // Get connection from pool to verify it's working
      const connection = await pool.getConnection();
      
      // Test the connection with a simple query
      await connection.query('SELECT 1 as health_check');
      connection.release();
      
      // Get pool status
      const status = getDatabaseStatus();
      
      console.log('✅ [CRON] Database connection health check passed');
      console.log('📊 [CRON] Database status:', {
        connected: status.connected,
        poolSize: status.poolSize,
        freeConnections: status.freeConnections,
        timestamp: new Date().toISOString(),
        istTime: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
      });
    } catch (error: any) {
      console.error('❌ [CRON] Database connection health check failed:', error.message);
      console.error('❌ [CRON] Error details:', {
        code: error.code,
        errno: error.errno,
        sqlState: error.sqlState,
        timestamp: new Date().toISOString(),
        istTime: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
      });
    }
  }, {
    scheduled: true,
    timezone: 'Asia/Kolkata', // Indian Standard Time
  });

  // Store task reference for cleanup
  cronTasks.push(dbHealthCheckTask);

  // Database backup - runs daily at 2 AM IST (before health check)
  const dbBackupTask = cron.schedule('0 2 * * *', async () => {
    console.log('⏰ [CRON] Running daily database backup at 2 AM IST...');
    const backupPath = await createDatabaseBackup();
    if (backupPath) {
      console.log('✅ [CRON] Database backup completed successfully');
    } else {
      console.error('❌ [CRON] Database backup failed');
    }
  }, {
    scheduled: true,
    timezone: 'Asia/Kolkata', // Indian Standard Time
  });

  // Store task reference for cleanup
  cronTasks.push(dbBackupTask);

  console.log('✅ Cron jobs initialized');
  console.log('📅 Scheduled jobs:');
  console.log('   - Database backup: Daily at 2:00 AM IST (Asia/Kolkata timezone)');
  console.log(`   - Next backup: ${getNextRunTime('0 2 * * *', 'Asia/Kolkata')}`);
  console.log('   - Database connection health check: Daily at 3:00 AM IST (Asia/Kolkata timezone)');
  console.log(`   - Next health check: ${getNextRunTime('0 3 * * *', 'Asia/Kolkata')}`);
}

/**
 * Get next run time for a cron expression
 */
function getNextRunTime(cronExpression: string, timezone: string): string {
  // This is a helper function - node-cron doesn't provide next run time directly
  // We'll calculate it manually or just show the schedule
  const now = new Date();
  const istNow = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
  const nextRun = new Date(istNow);
  
  // Extract hour from cron expression (format: "0 H * * *")
  const hourMatch = cronExpression.match(/^0 (\d+) \* \* \*/);
  const hour = hourMatch ? parseInt(hourMatch[1]) : 3;
  
  nextRun.setHours(hour, 0, 0, 0);
  
  // If it's already past the scheduled hour today, schedule for tomorrow
  if (istNow.getHours() >= hour) {
    nextRun.setDate(nextRun.getDate() + 1);
  }
  
  return nextRun.toLocaleString('en-IN', { 
    timeZone: 'Asia/Kolkata',
    dateStyle: 'full',
    timeStyle: 'short'
  });
}

/**
 * Stop all cron jobs
 * Call this when the app is shutting down
 */
export function stopCronJobs(): void {
  console.log('⏰ Stopping cron jobs...');
  
  let stoppedCount = 0;
  cronTasks.forEach((task, index) => {
    try {
      task.stop();
      stoppedCount++;
      console.log(`   ✅ Stopped cron task ${index + 1}`);
    } catch (error: any) {
      console.error(`   ❌ Error stopping cron task ${index + 1}:`, error.message);
    }
  });
  
  // Clear the tasks array
  cronTasks.length = 0;
  
  console.log(`✅ Stopped ${stoppedCount} cron job(s)`);
}

