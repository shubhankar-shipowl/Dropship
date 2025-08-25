// import { Pool, neonConfig } from '@neondatabase/serverless';
// import { drizzle } from 'drizzle-orm/neon-serverless';
// import ws from "ws";
// import * as schema from "@shared/schema";

// neonConfig.webSocketConstructor = ws;

// // Support both DB_URL and DATABASE_URL for backward compatibility
// const databaseUrl = process.env.DB_URL || process.env.DATABASE_URL;

// if (!databaseUrl) {
//   throw new Error(
//     "DB_URL or DATABASE_URL must be set. Did you forget to provision a database?",
//   );
// }

// export const pool = new Pool({ connectionString: databaseUrl });
// export const db = drizzle({ client: pool, schema });

import pg from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import * as schema from '@shared/schema';

const { Pool } = pg;

const db_url = process.env.DATABASE_URL;

if (!db_url) {
  throw new Error(
    'DATABASE_URL must be set. Did you forget to provision a database?',
  );
}

export const pool = new Pool({ connectionString: db_url });
export const db = drizzle(pool, { schema });
