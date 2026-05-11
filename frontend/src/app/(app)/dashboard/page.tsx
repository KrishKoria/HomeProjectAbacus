"use client";

import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { AppShell } from "@/components/app-shell";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { DatabricksStatus } from "@/lib/databricks/types";
import { MagnifyingGlass, Circle, CaretDown } from "@phosphor-icons/react";

export default function DashboardPage() {
  const router = useRouter();
  const searchRef = useRef<HTMLInputElement>(null);
  const [claimId, setClaimId] = useState("");

  const sessionQuery = useQuery({
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

  useEffect(() => {
    if (sessionQuery.isFetched && !sessionQuery.data) {
      router.push("/sign-in");
    }
  }, [sessionQuery.isFetched, sessionQuery.data, router]);

  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === "/" && document.activeElement !== searchRef.current) {
        e.preventDefault();
        searchRef.current?.focus();
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  function handleAnalyze() {
    if (claimId.trim()) {
      router.push(`/claims/${claimId.trim()}`);
    }
  }

  if (sessionQuery.isLoading) {
    return (
      <AppShell>
        <div className="space-y-4">
          <Skeleton className="h-8 w-48" />
          <Skeleton className="h-10 w-full max-w-md" />
        </div>
      </AppShell>
    );
  }

  if (!sessionQuery.data) {
    return null;
  }

  return (
    <AppShell>
      <div className="space-y-8">
        <div className="flex items-center justify-between">
          <h1 className="type-headline">Dashboard</h1>
          {statusQuery.data && (
            <div className="flex items-center gap-3 text-xs text-muted-foreground">
              <StatusDot name="OAuth" ok={statusQuery.data.oauth} />
              <StatusDot name="SQL" ok={statusQuery.data.sqlWarehouse} />
              <StatusDot name="Model" ok={statusQuery.data.analysisEndpoint} />
            </div>
          )}
        </div>

        <section>
          <div className="flex items-center gap-3">
            <div className="relative flex-1 max-w-md">
              <Input
                ref={searchRef}
                placeholder="Enter claim ID..."
                value={claimId}
                onChange={(e) => setClaimId(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleAnalyze()}
                className="pr-24"
              />
              <kbd className="absolute right-2 top-1/2 -translate-y-1/2 text-[10px] text-muted-foreground font-mono pointer-events-none">
                /
              </kbd>
            </div>
            <Button onClick={handleAnalyze} disabled={!claimId.trim()}>
              <MagnifyingGlass />
              Analyze
            </Button>
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            Press / to focus search
          </p>
        </section>

        <section className="space-y-3">
          <h2 className="type-title">Claims Queue</h2>

          {samplesQuery.isLoading && (
            <div className="space-y-2">
              {Array.from({ length: 5 }).map((_, i) => (
                <Skeleton key={i} className="h-10 w-full" />
              ))}
            </div>
          )}

          {samplesQuery.isError && (
            <div className="flex items-center gap-4 py-4 px-4 border border-border">
              <p className="text-sm text-muted-foreground flex-1">
                Unable to load sample claims.
              </p>
              <Button variant="outline" size="sm" onClick={() => samplesQuery.refetch()}>
                Retry
              </Button>
            </div>
          )}

          {samplesQuery.data && samplesQuery.data.claimIds.length === 0 && (
            <div className="py-12 text-center border border-border">
              <p className="type-body text-muted-foreground mx-auto">
                No claims analyzed yet. Enter a claim ID above to begin.
              </p>
            </div>
          )}

          {samplesQuery.data && samplesQuery.data.claimIds.length > 0 && (
            <div className="border border-border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="type-label">Claim ID</TableHead>
                    <TableHead className="type-label">Source</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {samplesQuery.data.claimIds.map((id, i) => (
                    <TableRow
                      key={id}
                      className="cursor-pointer hover:bg-muted/50 animate-in fade-in slide-in-from-bottom-1"
                      style={{ animationDelay: `${i * 30}ms`, animationFillMode: "backwards" }}
                      onClick={() => router.push(`/claims/${id}`)}
                    >
                      <TableCell className="type-mono font-medium">{id}</TableCell>
                      <TableCell>
                        <Badge variant="outline" className="text-xs">
                          sample
                        </Badge>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </section>

        <Collapsible className="space-y-2">
          <CollapsibleTrigger className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground transition-colors">
            <CaretDown className="size-3 transition-transform duration-200 group-data-[state=open]:rotate-180" />
            Runtime Status
          </CollapsibleTrigger>
          <CollapsibleContent>
            <div className="flex items-center gap-4 text-sm text-muted-foreground pt-2">
              {statusQuery.data ? (
                <>
                  <StatusBadge name="OAuth" ok={statusQuery.data.oauth} />
                  <StatusBadge name="SQL" ok={statusQuery.data.sqlWarehouse} />
                  <StatusBadge name="Model" ok={statusQuery.data.analysisEndpoint} />
                </>
              ) : (
                <Skeleton className="h-6 w-48" />
              )}
            </div>
          </CollapsibleContent>
        </Collapsible>
      </div>
    </AppShell>
  );
}

function StatusDot({ name, ok }: { name: string; ok: boolean }) {
  return (
    <span className="flex items-center gap-1.5">
      <Circle
        size={8}
        weight="fill"
        className={ok ? "text-status-ok" : "text-status-err"}
        aria-label={`${name}: ${ok ? "connected" : "disconnected"}`}
      />
      {name}
    </span>
  );
}

function StatusBadge({ name, ok }: { name: string; ok: boolean }) {
  return (
    <span className="flex items-center gap-1.5">
      <Circle
        size={8}
        weight="fill"
        className={ok ? "text-status-ok" : "text-status-err"}
        aria-label={`${name}: ${ok ? "connected" : "disconnected"}`}
      />
      <span>{name}</span>
      {ok ? null : <span className="text-status-err">offline</span>}
    </span>
  );
}
