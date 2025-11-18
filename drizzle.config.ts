import { defineConfig } from "drizzle-kit";

// Build connection config from individual env vars or use DATABASE_URL
let dbCredentials: { url?: string; host?: string; port?: number; user?: string; password?: string; database?: string };

if (process.env.DATABASE_URL) {
  dbCredentials = { url: process.env.DATABASE_URL };
} else if (process.env.DB_HOST && process.env.DB_USER && process.env.DB_PASSWORD && process.env.DB_NAME) {
  // Build connection string from individual env vars
  const password = encodeURIComponent(process.env.DB_PASSWORD);
  const user = encodeURIComponent(process.env.DB_USER);
  const host = process.env.DB_HOST;
  const port = process.env.DB_PORT || '3306';
  const database = process.env.DB_NAME;
  dbCredentials = {
    url: `mysql://${user}:${password}@${host}:${port}/${database}`,
  };
} else {
  throw new Error("DATABASE_URL or DB_HOST, DB_USER, DB_PASSWORD, DB_NAME must be set. Ensure the database is provisioned");
}

export default defineConfig({
  out: "./migrations",
  schema: "./shared/schema.ts",
  dialect: "mysql",
  dbCredentials,
  verbose: true,
  strict: true,
  introspect: {
    casing: "snake_case",
  },
});
