import cron from 'node-cron';
import { pool, getDatabaseStatus } from './db';

// Store cron task references for proper cleanup
const cronTasks: cron.ScheduledTask[] = [];

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

  console.log('✅ Cron jobs initialized');
  console.log('📅 Scheduled jobs:');
  console.log('   - Database connection health check: Daily at 3:00 AM IST (Asia/Kolkata timezone)');
  console.log(`   - Next run: ${getNextRunTime('0 3 * * *', 'Asia/Kolkata')}`);
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
  nextRun.setHours(3, 0, 0, 0);
  
  // If it's already past 3 AM today, schedule for tomorrow
  if (istNow.getHours() >= 3) {
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

