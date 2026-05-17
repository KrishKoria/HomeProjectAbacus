import { z } from "zod";

const envSchema = z.object({
  BETTER_AUTH_SECRET: z.string().min(32),
  BETTER_AUTH_URL: z.string().url(),
  BETTER_AUTH_TRUSTED_ORIGINS: z.string().optional(),
  GOOGLE_CLIENT_ID: z.string().min(1),
  GOOGLE_CLIENT_SECRET: z.string().min(1),
  DATABASE_URL: z.string().url().optional(),
  CLOUD_SQL_CONNECTION_NAME: z.string().min(1).optional(),
  DB_USER: z.string().min(1).optional(),
  DB_PASSWORD: z.string().min(1).optional(),
  DB_NAME: z.string().min(1).optional(),
  DB_PORT: z.coerce.number().int().positive().default(5432),
  DATABRICKS_HOST: z.string().url(),
  DATABRICKS_CLIENT_ID: z.string().min(1),
  DATABRICKS_CLIENT_SECRET: z.string().min(1),
  DATABRICKS_SQL_WAREHOUSE_ID: z.string().min(1),
  DATABRICKS_SQL_HTTP_PATH: z.string().min(1),
  CLAIMOPS_FEATURE_TABLE: z.string().min(1),
  CLAIMOPS_ANALYSIS_ENDPOINT: z.string().min(1),
  CLAIMOPS_ALLOWED_EMAIL_DOMAINS: z.string().min(1),
  CLAIMOPS_BOOTSTRAP_ADMIN_EMAILS: z.string().min(1),
  CLAIMOPS_CHAT_MODEL: z.string().default("databricks-meta-llama-3-3-70b-instruct"),
  CLAIMOPS_APP_ORIGIN: z.string().url().optional(),
  CLAIMOPS_GCS_LANDING_BUCKET: z.string().min(1),
  CLAIMOPS_GCS_LANDING_PREFIX: z.string().default("claimops-raw-landing"),
  CLAIMOPS_UPLOAD_CSV_MAX_BYTES: z.coerce.number().int().positive().default(100_000_000),
  CLAIMOPS_UPLOAD_PDF_MAX_BYTES: z.coerce.number().int().positive().default(50_000_000),
  CLAIMOPS_UPLOAD_SIGNED_POLICY_TTL_SECONDS: z.coerce.number().int().positive().default(900),
}).superRefine((values, ctx) => {
  if (values.DATABASE_URL) {
    return;
  }

  const requiredCloudSqlKeys: Array<keyof typeof values> = [
    "CLOUD_SQL_CONNECTION_NAME",
    "DB_USER",
    "DB_PASSWORD",
    "DB_NAME",
  ];

  for (const key of requiredCloudSqlKeys) {
    if (!values[key]) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: [key],
        message: `${key} is required when DATABASE_URL is not set`,
      });
    }
  }
});

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
