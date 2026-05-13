"use client";

import { Button } from "@/components/ui/button";

export function ClaimOpsErrorBoundary({
  error,
  reset,
}: {
  error: Error;
  reset: () => void;
}) {
  return (
    <div className="flex flex-1 items-center justify-center p-8" role="alert">
      <div className="max-w-md space-y-4 text-center">
        <h2 className="type-title text-status-err">Something went wrong</h2>
        <p className="text-sm text-muted-foreground">{error.message}</p>
        <Button variant="outline" onClick={reset}>
          Retry
        </Button>
      </div>
    </div>
  );
}
