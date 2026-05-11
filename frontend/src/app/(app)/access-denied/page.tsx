import Link from "next/link";

export default function AccessDeniedPage() {
  return (
    <div className="flex min-h-screen bg-[oklch(0.15_0.02_260)] text-[oklch(0.94_0.01_260)] items-center justify-center p-8">
      <div className="max-w-md text-center space-y-6">
        <h1 className="type-headline text-balance">Access Denied</h1>
        <p className="type-body text-white/60 mx-auto">
          Your account is not authorized to access ClaimOps. Contact your
          administrator if you believe this is an error.
        </p>
        <Link
          href="/sign-in"
          className="text-sm text-white/40 hover:text-white/80 underline underline-offset-4"
        >
          Return to sign in
        </Link>
      </div>
    </div>
  );
}
