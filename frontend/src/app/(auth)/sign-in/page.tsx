"use client";

import { authClient } from "@/lib/auth-client";
import { Button } from "@/components/ui/button";
import { GoogleLogo } from "@phosphor-icons/react";

export default function SignInPage() {
  return (
    <main className="flex min-h-screen">
      <div className="hidden md:flex w-[40%] bg-auth-bg text-auth-text flex-col justify-between p-12">
        <div className="space-y-6">
          <h1 className="type-display text-balance">ClaimOps</h1>
          <p className="type-headline max-w-sm text-auth-text/70">
            Claim Denial Risk Assessment Console
          </p>
        </div>
        <p className="type-body text-auth-text/50 max-w-sm">
          AI-powered claim denial risk scoring and remediation. A pre-submission
          quality gate that intercepts every claim before it reaches the insurer.
        </p>
      </div>

      <div className="flex-1 flex items-center justify-center p-8">
        <div className="w-full max-w-sm space-y-8">
          <div className="space-y-2">
            <h2 className="type-title">Sign in</h2>
            <p className="text-sm text-muted-foreground">
              Authorized billing analysts and supervisors only.
            </p>
          </div>
          <Button
            size="lg"
            className="w-full gap-3"
            onClick={() => authClient.signIn.social({ provider: "google" })}
          >
            <GoogleLogo className="size-4" aria-hidden="true" />
            Sign in with Google
          </Button>
        </div>
      </div>
    </main>
  );
}
