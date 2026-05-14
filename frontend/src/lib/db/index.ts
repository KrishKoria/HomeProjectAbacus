import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import * as schema from "./schema";

function resolveConnectionConfig() {
  const directUrl = process.env.DATABASE_URL?.trim();
  if (directUrl) {
    return directUrl;
  }

  const connectionName = process.env.CLOUD_SQL_CONNECTION_NAME?.trim();
  const user = process.env.DB_USER?.trim();
  const password = process.env.DB_PASSWORD?.trim();
  const database = process.env.DB_NAME?.trim();
  const portRaw = process.env.DB_PORT?.trim();

  if (!connectionName || !user || !password || !database) {
    throw new Error(
      "Database configuration is incomplete. Set DATABASE_URL or CLOUD_SQL_CONNECTION_NAME, DB_USER, DB_PASSWORD, and DB_NAME.",
    );
  }

  const parsedPort = Number(portRaw ?? "5432");
  const port = Number.isFinite(parsedPort) ? parsedPort : 5432;

  return {
    host: `/cloudsql/${connectionName}`,
    user,
    password,
    database,
    port,
  };
}

function createDatabase() {
  const connectionConfig = resolveConnectionConfig();
  const queryClient =
    typeof connectionConfig === "string"
      ? postgres(connectionConfig)
      : postgres(connectionConfig);
  return drizzle(queryClient, { schema });
}

type Database = ReturnType<typeof createDatabase>;

let database: Database | null = null;

export function getDb(): Database {
  database ??= createDatabase();
  return database;
}
