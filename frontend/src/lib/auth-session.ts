import { headers } from "next/headers";
import { auth } from "@/lib/auth";
import { env } from "@/lib/server/env";

type Session = typeof auth.$Infer.Session;

function isAuthorized(email: string | null | undefined): boolean {
  if (!email) return false;
  const adminEmails = env.CLAIMOPS_BOOTSTRAP_ADMIN_EMAILS.split(",").map((e) => e.trim());
  if (adminEmails.includes(email)) return true;
  const allowedDomains = env.CLAIMOPS_ALLOWED_EMAIL_DOMAINS.split(",").map((d) => d.trim());
  if (allowedDomains.includes("*")) return true;
  const domain = email.split("@")[1];
  if (domain && allowedDomains.includes(domain)) return true;
  return false;
}

export async function getOptionalSession(): Promise<Session | null> {
  try {
    const h = await headers();
    const session = await auth.api.getSession({ headers: h });
    return session;
  } catch {
    return null;
  }
}

export async function requireSession(): Promise<Session> {
  const session = await getOptionalSession();
  if (!session) {
    throw new Error("Unauthorized");
  }
  return session;
}

export async function requireAuthorizedSession(): Promise<Session> {
  const session = await requireSession();
  if (!isAuthorized(session.user.email)) {
    throw new Error("Forbidden");
  }
  return session;
}
