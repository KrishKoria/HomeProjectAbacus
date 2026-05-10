import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("@/lib/server/env", () => ({
  env: new Proxy(
    {},
    {
      get(_, prop: string) {
        const vars: Record<string, string> = {
          BETTER_AUTH_SECRET: "abcdefghijklmnopqrstuvwxyz0123456789abcd",
          BETTER_AUTH_URL: "http://localhost:3000",
          GOOGLE_CLIENT_ID: "test-client-id",
          GOOGLE_CLIENT_SECRET: "test-client-secret",
          DATABASE_URL: "postgresql://user:pass@localhost:5433/db",
          DATABRICKS_HOST: "https://test.databricks.com",
          DATABRICKS_CLIENT_ID: "test-client-id",
          DATABRICKS_CLIENT_SECRET: "test-client-secret",
          DATABRICKS_SQL_WAREHOUSE_ID: "test-warehouse",
          DATABRICKS_SQL_HTTP_PATH: "/sql/1.0/warehouses/test",
          CLAIMOPS_FEATURE_TABLE: "test.features",
          CLAIMOPS_ANALYSIS_ENDPOINT: "test-endpoint",
          CLAIMOPS_ALLOWED_EMAIL_DOMAINS: "example.com",
          CLAIMOPS_BOOTSTRAP_ADMIN_EMAILS: "admin@example.com",
        };
        return vars[prop];
      },
    },
  ),
}));

const mockFetch = vi.fn();
globalThis.fetch = mockFetch;

beforeEach(() => {
  mockFetch.mockReset();
});

describe("Databricks OAuth", () => {
  it("should request and cache tokens", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({ access_token: "test-token", expires_in: 3600 }), {
        status: 200,
      }),
    );

    const { getToken, resetTokenCache } = await import(
      "@/lib/databricks/oauth"
    );

    resetTokenCache();
    const token = await getToken();
    expect(token).toBe("test-token");
    expect(mockFetch).toHaveBeenCalledTimes(1);

    const token2 = await getToken();
    expect(token2).toBe("test-token");
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("should fetch new token after cache expires", async () => {
    mockFetch
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ access_token: "first-token", expires_in: 0 }), {
          status: 200,
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ access_token: "second-token", expires_in: 3600 }), {
          status: 200,
        }),
      );

    const { getToken, resetTokenCache } = await import(
      "@/lib/databricks/oauth"
    );

    resetTokenCache();
    const t1 = await getToken();
    expect(t1).toBe("first-token");
    const t2 = await getToken();
    expect(t2).toBe("second-token");
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });
});

describe("Databricks client error formatting", () => {
  it("should strip secrets from error messages", async () => {
    const { databricksFetch } = await import("@/lib/databricks/client");

    mockFetch.mockRejectedValue(
      new Error('Request failed with "Bearer secret123" in the log'),
    );

    const result = await databricksFetch("/test", { timeout: 100 });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.message).not.toContain("secret123");
    }
  });
});
