import { defineConfig } from "drizzle-kit";

function resolveDatabaseUrl(): string {
  const directUrl = process.env.DATABASE_URL?.trim();
  if (directUrl) {
    return directUrl;
  }

  const host = process.env.DB_HOST?.trim();
  const user = process.env.DB_USER?.trim();
  const password =
    process.env.DB_PASSWORD?.trim() ?? process.env.DB_PASS?.trim();
  const database = process.env.DB_NAME?.trim();
  const port = process.env.DB_PORT?.trim() || "5432";

  if (!host || !user || !password || !database) {
    throw new Error(
      "Set DATABASE_URL or DB_HOST, DB_USER, DB_PASSWORD, and DB_NAME before running drizzle-kit.",
    );
  }

  const url = new URL(`postgresql://${host}/${database}`);
  url.username = user;
  url.password = password;
  url.port = port;
  return url.toString();
}

export default defineConfig({
  schema: "./src/lib/db/schema.ts",
  out: "./drizzle",
  dialect: "postgresql",
  dbCredentials: {
    url: resolveDatabaseUrl(),
  },
});
