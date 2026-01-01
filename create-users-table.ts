/**
 * Script to create the users table if it doesn't exist
 * This is a quick fix to add the users table to the database
 */

import 'dotenv/config';
import { pool, initializeDatabase, closeDatabase } from './server/db';

async function createUsersTable() {
  try {
    await initializeDatabase();
    
    console.log('📋 Creating users table...');
    
    const createTableSQL = `
      CREATE TABLE IF NOT EXISTS \`users\` (
        \`id\` varchar(36) NOT NULL DEFAULT (UUID()),
        \`username\` varchar(255) NOT NULL,
        \`email\` varchar(255) NOT NULL,
        \`password\` text NOT NULL,
        \`created_at\` timestamp NOT NULL DEFAULT (now()),
        \`updated_at\` timestamp NOT NULL DEFAULT (now()) ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (\`id\`),
        UNIQUE KEY \`users_username_unique\` (\`username\`),
        UNIQUE KEY \`users_email_unique\` (\`email\`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    `;
    
    await pool.query(createTableSQL);
    
    console.log('✅ Users table created successfully!');
    
    // Verify the table was created
    const [tables] = await pool.query(
      "SHOW TABLES LIKE 'users'"
    ) as any[];
    
    if (tables.length > 0) {
      console.log('✅ Verified: users table exists');
    } else {
      console.warn('⚠️ Warning: Could not verify users table creation');
    }
    
    process.exit(0);
  } catch (error: any) {
    console.error('❌ Error creating users table:', error.message || error);
    if (error.code === 'ER_TABLE_EXISTS_ERROR') {
      console.log('ℹ️ Users table already exists, nothing to do');
      process.exit(0);
    }
    process.exit(1);
  } finally {
    await closeDatabase();
  }
}

createUsersTable();

