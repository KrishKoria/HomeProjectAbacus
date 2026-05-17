import { afterEach, describe, expect, it, vi } from "vitest";

const DB_ENV_KEYS = [
  "DATABASE_URL",
  "CLOUD_SQL_CONNECTION_NAME",
  "DB_USER",
  "DB_PASSWORD",
  "DB_NAME",
  "DB_PORT",
] as const;

const savedEnv = new Map<string, string | undefined>();

afterEach(() => {
  for (const key of DB_ENV_KEYS) {
    const value = savedEnv.get(key);
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }
  savedEnv.clear();
});

describe("runtime environment loading", () => {
  it("does not require database env while importing the Better Auth route", async () => {
    vi.resetModules();
    for (const key of DB_ENV_KEYS) {
      savedEnv.set(key, process.env[key]);
      delete process.env[key];
    }

    await expect(import("@/app/api/auth/[...all]/route")).resolves.toMatchObject({
      GET: expect.any(Function),
      POST: expect.any(Function),
    });
  }, 20_000);
});
