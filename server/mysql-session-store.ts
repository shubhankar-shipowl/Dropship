import { Store } from 'express-session';
import mysql from 'mysql2/promise';

interface SessionData {
  [key: string]: any;
}

export class MySQLSessionStore extends Store {
  private pool: mysql.Pool;
  private tableName: string;

  constructor(pool: mysql.Pool, tableName: string = 'user_sessions') {
    super();
    this.pool = pool;
    this.tableName = tableName;
    this.initTable();
  }

  private async initTable() {
    try {
      await this.pool.execute(`
        CREATE TABLE IF NOT EXISTS \`${this.tableName}\` (
          \`session_id\` VARCHAR(255) NOT NULL PRIMARY KEY,
          \`expires\` BIGINT NOT NULL,
          \`data\` TEXT,
          INDEX \`expires_idx\` (\`expires\`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
      `);
    } catch (error) {
      console.error('Error creating session table:', error);
    }
  }

  async get(sessionId: string, callback: (err?: any, session?: SessionData) => void) {
    try {
      const [rows] = await this.pool.execute(
        `SELECT data FROM \`${this.tableName}\` WHERE session_id = ? AND expires > ?`,
        [sessionId, Date.now()]
      ) as [mysql.RowDataPacket[], mysql.FieldPacket[]];

      if (rows.length === 0) {
        return callback();
      }

      const sessionData = rows[0].data;
      const session = sessionData ? JSON.parse(sessionData) : {};
      callback(null, session);
    } catch (error) {
      callback(error);
    }
  }

  async set(sessionId: string, session: SessionData, callback?: (err?: any) => void) {
    try {
      const expires = (session.cookie?.expires 
        ? new Date(session.cookie.expires).getTime() 
        : Date.now() + (24 * 60 * 60 * 1000)); // Default 24 hours

      const data = JSON.stringify(session);

      await this.pool.execute(
        `INSERT INTO \`${this.tableName}\` (session_id, expires, data) 
         VALUES (?, ?, ?) 
         ON DUPLICATE KEY UPDATE expires = ?, data = ?`,
        [sessionId, expires, data, expires, data]
      );

      if (callback) callback();
    } catch (error) {
      if (callback) callback(error);
    }
  }

  async destroy(sessionId: string, callback?: (err?: any) => void) {
    try {
      await this.pool.execute(
        `DELETE FROM \`${this.tableName}\` WHERE session_id = ?`,
        [sessionId]
      );
      if (callback) callback();
    } catch (error) {
      if (callback) callback(error);
    }
  }

  async all(callback: (err?: any, sessions?: { [sid: string]: SessionData } | null) => void) {
    try {
      const [rows] = await this.pool.execute(
        `SELECT session_id, data FROM \`${this.tableName}\` WHERE expires > ?`,
        [Date.now()]
      ) as [mysql.RowDataPacket[], mysql.FieldPacket[]];

      const sessions: { [sid: string]: SessionData } = {};
      for (const row of rows) {
        sessions[row.session_id] = row.data ? JSON.parse(row.data) : {};
      }
      callback(null, sessions);
    } catch (error) {
      callback(error);
    }
  }

  async length(callback: (err?: any, length?: number) => void) {
    try {
      const [rows] = await this.pool.execute(
        `SELECT COUNT(*) as count FROM \`${this.tableName}\` WHERE expires > ?`,
        [Date.now()]
      ) as [mysql.RowDataPacket[], mysql.FieldPacket[]];

      const length = rows[0]?.count || 0;
      callback(null, length);
    } catch (error) {
      callback(error);
    }
  }

  async clear(callback?: (err?: any) => void) {
    try {
      await this.pool.execute(`TRUNCATE TABLE \`${this.tableName}\``);
      if (callback) callback();
    } catch (error) {
      if (callback) callback(error);
    }
  }

  async touch(sessionId: string, session: SessionData, callback?: (err?: any) => void) {
    try {
      const expires = (session.cookie?.expires 
        ? new Date(session.cookie.expires).getTime() 
        : Date.now() + (24 * 60 * 60 * 1000));

      await this.pool.execute(
        `UPDATE \`${this.tableName}\` SET expires = ? WHERE session_id = ?`,
        [expires, sessionId]
      );
      if (callback) callback();
    } catch (error) {
      if (callback) callback(error);
    }
  }
}

