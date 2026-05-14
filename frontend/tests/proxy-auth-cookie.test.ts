import { describe, expect, it } from "vitest";
import { NextRequest } from "next/server";
import { proxy } from "@/proxy";

function requestFor(path: string, cookie?: string) {
  return new NextRequest(`https://example.com${path}`, {
    headers: cookie ? { cookie } : undefined,
  });
}

describe("proxy auth cookie gate", () => {
  it("redirects protected routes without a session cookie", () => {
    const response = proxy(requestFor("/claims"));

    expect(response.status).toBe(307);
    expect(response.headers.get("location")).toBe("https://example.com/sign-in");
  });

  it("allows protected routes with Better Auth secure session cookies", () => {
    const response = proxy(
      requestFor("/claims", "__Secure-better-auth.session_token=session-value"),
    );

    expect(response.headers.get("x-middleware-next")).toBe("1");
    expect(response.headers.get("location")).toBeNull();
  });
});
