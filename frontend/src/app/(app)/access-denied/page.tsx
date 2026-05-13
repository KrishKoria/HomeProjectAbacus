import Link from "next/link";

export default function AccessDeniedPage() {
  return (
    <main className="flex min-h-screen bg-auth-bg text-auth-text items-center justify-center p-8">
      <div className="max-w-md text-center space-y-6">
        <h1 className="type-headline text-balance">Access Denied</h1>
        <p className="type-body text-auth-text/60 mx-auto">
          Your account is not authorized to access ClaimOps. Contact your
          administrator if you believe this is an error.
        </p>
        <Link
          href="/sign-in"
          className="text-sm text-auth-text/60 hover:text-auth-text/80 underline underline-offset-4"
        >
          Return to sign in
        </Link>
      </div>
    </main>
  );
}
