import { z } from "zod";

const envSchema = z.object({
  BETTER_AUTH_SECRET: z.string().min(32),
  BETTER_AUTH_URL: z.string().url(),
  GOOGLE_CLIENT_ID: z.string().min(1),
  GOOGLE_CLIENT_SECRET: z.string().min(1),
  DATABASE_URL: z.string().url().refine(
    (url) => {
      try {
        const u = new URL(url);
        return u.port === "5433" || u.hostname.includes("pooler");
      } catch {
        return true;
      }
    },
    { message: "DATABASE_URL should use Neon pooled connection (port 5433 or -pooler hostname), not direct port 5432" }
  ),
  DATABRICKS_HOST: z.string().url(),
  DATABRICKS_CLIENT_ID: z.string().min(1),
  DATABRICKS_CLIENT_SECRET: z.string().min(1),
  DATABRICKS_SQL_WAREHOUSE_ID: z.string().min(1),
  DATABRICKS_SQL_HTTP_PATH: z.string().min(1),
  CLAIMOPS_FEATURE_TABLE: z.string().min(1),
  CLAIMOPS_ANALYSIS_ENDPOINT: z.string().min(1),
  CLAIMOPS_ALLOWED_EMAIL_DOMAINS: z.string().min(1),
  CLAIMOPS_BOOTSTRAP_ADMIN_EMAILS: z.string().min(1),
});

declare global {
  namespace NodeJS {
    interface ProcessEnv extends z.input<typeof envSchema> {}
  }
}

function createEnv() {
  const result = envSchema.safeParse(process.env);
  if (!result.success) {
    console.error("Invalid environment variables:");
    for (const issue of result.error.issues) {
      console.error(`  - ${issue.path.join(".")}: ${issue.message}`);
    }
    throw new Error("Invalid environment variables");
  }
  return result.data;
}

let _env: ReturnType<typeof createEnv> | null = null;

export function getEnv() {
  if (!_env) {
    _env = createEnv();
  }
  return _env;
}

export const env = new Proxy({} as ReturnType<typeof createEnv>, {
  get(_, prop: string) {
    return getEnv()[prop as keyof ReturnType<typeof createEnv>];
  },
});
