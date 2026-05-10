"use client";

import { Shield } from "@phosphor-icons/react";

export default function AccessDeniedPage() {
  return (
    <div className="flex flex-1 items-center justify-center">
      <div className="flex flex-col items-center gap-4 max-w-sm text-center">
        <Shield size={40} />
        <h1 className="text-xl font-semibold tracking-tight">Access Denied</h1>
        <p className="text-sm text-muted-foreground">
          Your account does not have permission to access ClaimOps. Contact your
          administrator if you believe this is an error.
        </p>
      </div>
    </div>
  );
}
