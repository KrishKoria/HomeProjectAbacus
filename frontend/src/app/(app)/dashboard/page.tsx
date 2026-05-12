"use client";

import { useEffect, useRef, useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { AppShell } from "@/components/app-shell";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import type { DatabricksStatus } from "@/lib/databricks/types";
import type { PaginatedClaims } from "@/lib/db/claims";
import { MagnifyingGlass, Circle, ArrowRight } from "@phosphor-icons/react";

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

  const claimsQuery = useQuery({
    queryKey: ["claims"],
    queryFn: async () => {
      const res = await fetch("/api/claims");
      if (!res.ok) throw new Error("Failed to load claims");
      return res.json() as Promise<PaginatedClaims>;
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

  const stats = useMemo(() => {
    const claims = (claimsQuery.data?.claims ?? []).filter((c) => c.riskLevel !== null);
    return {
      total: claims.length,
      risk: {
        high: claims.filter((c) => c.riskLevel === "high").length,
        medium: claims.filter((c) => c.riskLevel === "medium").length,
        low: claims.filter((c) => c.riskLevel === "low").length,
      },
      status: {
        new: claims.filter((c) => c.status === "new").length,
        reviewed: claims.filter((c) => c.status === "reviewed").length,
        actioned: claims.filter((c) => c.status === "actioned").length,
      },
    };
  }, [claimsQuery.data]);

  if (sessionQuery.isLoading) {
    return (
      <AppShell>
        <div className="p-6 space-y-4">
          <Skeleton className="h-7 w-32" />
          <Skeleton className="h-10 w-full max-w-md" />
        </div>
      </AppShell>
    );
  }

  if (!sessionQuery.data) return null;

  return (
    <AppShell>
      <div className="p-6 space-y-8 max-w-3xl">
        <div className="flex items-baseline justify-between">
          <h1 className="type-headline">Dashboard</h1>
          {statusQuery.data && (
            <div className="flex items-center gap-3 text-xs text-muted-foreground">
              <StatusDot name="OAuth" ok={statusQuery.data.oauth} />
              <StatusDot name="SQL" ok={statusQuery.data.sqlWarehouse} />
              <StatusDot name="Model" ok={statusQuery.data.analysisEndpoint} />
            </div>
          )}
        </div>

        {/* Quick analyze */}
        <section className="space-y-2">
          <div className="flex items-center gap-2">
            <div className="relative flex-1 max-w-sm">
              <Input
                ref={searchRef}
                placeholder="Enter claim ID…"
                value={claimId}
                onChange={(e) => setClaimId(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleAnalyze()}
                className="pr-8"
              />
              <kbd className="absolute right-2 top-1/2 -translate-y-1/2 text-[10px] text-muted-foreground font-mono pointer-events-none">
                /
              </kbd>
            </div>
            <Button onClick={handleAnalyze} disabled={!claimId.trim()}>
              <MagnifyingGlass data-icon="inline-start" />
              Analyze
            </Button>
          </div>
          <p className="type-caption text-muted-foreground">
            Press <kbd className="font-mono">/</kbd> to focus. Analyzes claim and opens detail view.
          </p>
        </section>

        {/* Queue overview */}
        <section className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="type-title">Queue Overview</h2>
            <button
              onClick={() => router.push("/claims")}
              className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              View all <ArrowRight className="size-3" />
            </button>
          </div>

          {claimsQuery.isLoading && (
            <div className="grid grid-cols-3 gap-px border border-border bg-border">
              {Array.from({ length: 6 }).map((_, i) => (
                <div key={i} className="bg-background px-5 py-4 space-y-1.5">
                  <Skeleton className="h-3 w-16" />
                  <Skeleton className="h-6 w-8" />
                </div>
              ))}
            </div>
          )}

          {claimsQuery.isError && (
            <div className="border border-border px-5 py-4">
              <p className="text-sm text-muted-foreground">Could not load queue metrics.</p>
            </div>
          )}

          {claimsQuery.data && stats.total > 0 && (
            <div className="space-y-px border border-border">
              <div className="grid grid-cols-3 divide-x divide-border">
                <StatCell label="High risk" value={stats.risk.high} total={stats.total} valueClass="text-risk-high" onClick={() => router.push("/claims?risk=high")} />
                <StatCell label="Medium risk" value={stats.risk.medium} total={stats.total} valueClass="text-risk-medium" onClick={() => router.push("/claims?risk=medium")} />
                <StatCell label="Low risk" value={stats.risk.low} total={stats.total} valueClass="text-risk-low" onClick={() => router.push("/claims?risk=low")} />
              </div>
              <div className="grid grid-cols-3 divide-x divide-border border-t border-border">
                <StatCell label="New" value={stats.status.new} total={stats.total} onClick={() => router.push("/claims?status=new")} />
                <StatCell label="Reviewed" value={stats.status.reviewed} total={stats.total} onClick={() => router.push("/claims?status=reviewed")} />
                <StatCell label="Actioned" value={stats.status.actioned} total={stats.total} onClick={() => router.push("/claims?status=actioned")} />
              </div>
            </div>
          )}

          {claimsQuery.data && stats.total === 0 && (
            <div className="border border-border py-10 text-center">
              <p className="type-body text-muted-foreground">
                Queue is empty. Visit Claims to populate it automatically.
              </p>
            </div>
          )}
        </section>
      </div>
    </AppShell>
  );
}

function StatCell({
  label,
  value,
  total,
  valueClass,
  onClick,
}: {
  label: string;
  value: number;
  total: number;
  valueClass?: string;
  onClick?: () => void;
}) {
  const pct = total > 0 ? Math.round((value / total) * 100) : 0;
  return (
    <button
      onClick={onClick}
      className="group px-5 py-4 text-left bg-background hover:bg-muted/50 transition-colors"
    >
      <p className="type-caption text-muted-foreground mb-1">{label}</p>
      <p className={`text-2xl font-semibold tabular-nums tracking-tight ${valueClass ?? "text-foreground"}`}>
        {value}
      </p>
      {total > 0 && (
        <p className="type-caption text-muted-foreground mt-0.5">{pct}%</p>
      )}
    </button>
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
