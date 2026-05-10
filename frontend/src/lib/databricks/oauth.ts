import { env } from "@/lib/server/env";

interface TokenCache {
  token: string;
  expiresAt: number;
}

let cache: TokenCache | null = null;
let refreshTimer: ReturnType<typeof setTimeout> | null = null;

async function requestToken(): Promise<TokenCache> {
  const host = env.DATABRICKS_HOST.replace(/\/+$/, "");
  const params = new URLSearchParams({
    grant_type: "client_credentials",
    scope: "all-apis",
    client_id: env.DATABRICKS_CLIENT_ID,
    client_secret: env.DATABRICKS_CLIENT_SECRET,
  });

  const response = await fetch(`${host}/oidc/v1/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: params,
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "(unreadable)");
    const msg = `Databricks OAuth failed [${response.status}]: ${body}`;
    console.error(msg);
    throw new Error(msg);
  }

  const data = await response.json();
  const expiresIn = data.expires_in ?? 3600;
  const now = Date.now();

  return {
    token: data.access_token,
    expiresAt: now + expiresIn * 1000,
  };
}

function scheduleRefresh(cacheEntry: TokenCache): void {
  if (refreshTimer) clearTimeout(refreshTimer);
  const ttl = cacheEntry.expiresAt - Date.now();
  const refreshAt = Math.max(0, ttl * 0.5);
  refreshTimer = setTimeout(async () => {
    try {
      cache = await requestToken();
    } catch {
      cache = null;
    }
  }, refreshAt);
}

export async function getToken(): Promise<string> {
  if (cache && Date.now() < cache.expiresAt) {
    return cache.token;
  }
  cache = await requestToken();
  scheduleRefresh(cache);
  return cache.token;
}

export function resetTokenCache(): void {
  cache = null;
  if (refreshTimer) {
    clearTimeout(refreshTimer);
    refreshTimer = null;
  }
}
