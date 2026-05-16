import Link from "next/link";
import { WarningIcon } from "@phosphor-icons/react/dist/ssr";

export default function AccessDeniedPage() {
  return (
    <main className="flex min-h-screen bg-background text-foreground items-center justify-center p-8">
      <div className="max-w-md text-center space-y-6">
        <WarningIcon size={32} className="mx-auto text-status-err" />
        <h1 className="type-headline text-balance">Access Denied</h1>
        <p className="type-body text-muted-foreground mx-auto">
          Your account is signed in but not authorized to use ClaimOps. If you
          believe this is an error, contact your administrator with your sign-in
          email.
        </p>
        <Link
          href="/sign-in"
          className="text-sm text-muted-foreground hover:text-foreground underline underline-offset-4"
        >
          Return to sign in
        </Link>
      </div>
    </main>
  );
}
