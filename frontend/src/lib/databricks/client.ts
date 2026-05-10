import { getToken } from "@/lib/databricks/oauth";
import { env } from "@/lib/server/env";

export interface ApiError {
  ok: false;
  status: number;
  message: string;
}

export interface ApiSuccess<T> {
  ok: true;
  data: T;
}

export type ApiResult<T> = ApiSuccess<T> | ApiError;

function stripSecrets(error: unknown): string {
  const msg = error instanceof Error ? error.message : String(error);
  return msg.replace(
    /(client_secret|access_token|Bearer\s+)\S+/gi,
    "$1***",
  );
}

export async function databricksFetch<T>(
  path: string,
  options: RequestInit & { timeout?: number } = {},
): Promise<ApiResult<T>> {
  const timeout = options.timeout ?? 30_000;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);

  try {
    const token = await getToken();
    const host = env.DATABRICKS_HOST.replace(/\/+$/, "");
    const url = `${host}${path}`;

    const response = await fetch(url, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        ...options.headers,
        Authorization: `Bearer ${token}`,
      },
      signal: controller.signal,
    });

    clearTimeout(timer);

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`[databricks] ${path} → ${response.status}: ${stripSecrets(errorText)}`);
      return {
        ok: false,
        status: response.status,
        message: stripSecrets(errorText),
      };
    }

    const data = (await response.json()) as T;
    return { ok: true, data };
  } catch (error) {
    clearTimeout(timer);
    const message = stripSecrets(error);
    console.error(`[databricks] ${path} → error: ${message}`);
    if (error instanceof DOMException && error.name === "AbortError") {
      return { ok: false, status: 504, message: "Databricks request timed out" };
    }
    return { ok: false, status: 502, message };
  }
}
