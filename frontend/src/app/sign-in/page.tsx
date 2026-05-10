"use client";

import { authClient } from "@/lib/auth-client";
import { Button } from "@/components/ui/button";
import { GoogleLogo } from "@phosphor-icons/react";

export default function SignInPage() {
  return (
    <div className="flex flex-1 items-center justify-center">
      <div className="flex flex-col items-center gap-6 max-w-sm text-center">
        <h1 className="text-xl font-semibold tracking-tight">ClaimOps</h1>
        <p className="text-sm text-muted-foreground">
          Sign in to access the claim denial analysis dashboard.
        </p>
        <Button
          size="lg"
          className="w-full"
          onClick={() => authClient.signIn.social({ provider: "google" })}
        >
          <GoogleLogo />
          Sign in with Google
        </Button>
      </div>
    </div>
  );
}
