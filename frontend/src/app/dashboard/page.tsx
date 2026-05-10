"use client";

import { useQuery } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { AppShell } from "@/components/app-shell";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import type { DatabricksStatus } from "@/lib/databricks/types";
import { MagnifyingGlass, Circle } from "@phosphor-icons/react";
import { useState } from "react";

export default function DashboardPage() {
  const router = useRouter();
  const [claimId, setClaimId] = useState("");

  const query = useQuery({
    queryKey: ["session"],
    queryFn: async () => {
      const res = await fetch("/api/me");
      if (!res.ok) return null;
      return res.json();
    },
  });

  const samplesQuery = useQuery({
    queryKey: ["claim-samples"],
    queryFn: async () => {
      const res = await fetch("/api/claims/samples");
      if (!res.ok) throw new Error("Failed to fetch samples");
      return res.json() as Promise<{ claimIds: string[] }>;
    },
  });

  const statusQuery = useQuery({
    queryKey: ["runtime-status"],
    queryFn: async () => {
      const res = await fetch("/api/runtime/status");
      if (!res.ok) throw new Error("Failed to fetch status");
      return res.json() as Promise<DatabricksStatus>;
    },
    refetchInterval: 30_000,
  });

  if (query.isLoading) {
    return (
      <AppShell>
        <div className="space-y-4">
          <Skeleton className="h-8 w-48" />
          <Skeleton className="h-10 w-full" />
        </div>
      </AppShell>
    );
  }

  return (
    <AppShell>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <h1 className="text-lg font-semibold tracking-tight">Dashboard</h1>
          {statusQuery.data && (
            <div className="flex items-center gap-3 text-xs text-muted-foreground">
              <span className="flex items-center gap-1">
                <Circle
                  size={8}
                  weight="fill"
                  className={statusQuery.data.oauth ? "text-green-500" : "text-red-500"}
                />
                OAuth
              </span>
              <span className="flex items-center gap-1">
                <Circle
                  size={8}
                  weight="fill"
                  className={statusQuery.data.sqlWarehouse ? "text-green-500" : "text-red-500"}
                />
                SQL
              </span>
              <span className="flex items-center gap-1">
                <Circle
                  size={8}
                  weight="fill"
                  className={statusQuery.data.analysisEndpoint ? "text-green-500" : "text-red-500"}
                />
                Model
              </span>
            </div>
          )}
        </div>

        <div className="flex gap-2">
          <Input
            placeholder="Enter claim ID..."
            value={claimId}
            onChange={(e) => setClaimId(e.target.value)}
            className="max-w-xs"
          />
          <Button
            onClick={() => router.push(`/claims/${claimId}`)}
            disabled={!claimId.trim()}
          >
            <MagnifyingGlass />
            Analyze
          </Button>
        </div>

        <div>
          <h2 className="text-sm font-medium mb-3">Sample Claims</h2>
          {samplesQuery.isLoading ? (
            <div className="space-y-2">
              {Array.from({ length: 5 }).map((_, i) => (
                <Skeleton key={i} className="h-10 w-64" />
              ))}
            </div>
          ) : samplesQuery.isError ? (
            <p className="text-sm text-muted-foreground">
              Unable to load sample claims. Check Databricks connection.
            </p>
          ) : (
            <div className="flex flex-wrap gap-2">
              {samplesQuery.data?.claimIds.map((id) => (
                <Badge
                  key={id}
                  variant="outline"
                  className="cursor-pointer"
                  onClick={() => router.push(`/claims/${id}`)}
                >
                  {id}
                </Badge>
              ))}
            </div>
          )}
        </div>
      </div>
    </AppShell>
  );
}
