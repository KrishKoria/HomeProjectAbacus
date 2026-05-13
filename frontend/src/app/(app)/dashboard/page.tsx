"use client";

import Link from "next/link";
import { useCallback, useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { AppShell } from "@/components/app-shell";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { RiskBar } from "@/components/risk-bar";
import type { DatabricksStatus } from "@/lib/databricks/types";
import type { ClaimReview, ClaimStats } from "@/lib/db/claims";
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

  const statsQuery = useQuery({
    queryKey: ["claim-stats"],
    queryFn: async () => {
      const res = await fetch("/api/claims/stats");
      if (!res.ok) throw new Error("Failed to load stats");
      return res.json() as Promise<ClaimStats>;
    },
  });

  const topClaimsQuery = useQuery({
    queryKey: ["top-claims"],
    queryFn: async () => {
      const res = await fetch("/api/claims/top");
      if (!res.ok) throw new Error("Failed to load top claims");
      return (res.json() as Promise<{ claims: ClaimReview[] }>).then(
        (d) => d.claims,
      );
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

  const handleAnalyze = useCallback(() => {
    if (claimId.trim()) {
      router.push(`/claims/${claimId.trim()}`);
    }
  }, [claimId, router]);

  const stats = statsQuery.data ?? null;
  const topClaims = topClaimsQuery.data ?? [];

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
      <div className="p-6 space-y-8 max-w-5xl">
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

        {/* Queue overview — 3-column strip */}
        <section className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="type-title">Queue Overview</h2>
            <button
              onClick={() => router.push("/claims")}
              className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              View all <ArrowRight className="size-3" aria-hidden="true" />
            </button>
          </div>

          {(statsQuery.isLoading || statsQuery.isPending) && (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              {Array.from({ length: 3 }).map((_, i) => (
                <div key={i} className="space-y-3">
                  <Skeleton className="h-3 w-24" />
                  <Skeleton className="h-2 w-full" />
                  <Skeleton className="h-2 w-full" />
                  <Skeleton className="h-2 w-full" />
                </div>
              ))}
            </div>
          )}

          {statsQuery.isError && (
            <div role="alert" className="border border-border px-5 py-4">
              <p className="type-body text-muted-foreground">
                Could not load queue metrics.
              </p>
            </div>
          )}

          {stats && stats.total === 0 && (
            <div
              role="status"
              className="border border-border py-10 text-center"
            >
              <p className="type-body text-muted-foreground">
                Queue is empty. Visit Claims to populate it automatically.
              </p>
            </div>
          )}

          {stats && stats.total > 0 && (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-8 border-t border-border pt-6">
              {/* Column 1 — Risk distribution */}
              <div className="space-y-3">
                <p className="type-label text-muted-foreground">
                  Risk distribution
                </p>
                <div className="space-y-2.5">
                  {(["high", "medium", "low"] as const).map((level) => {
                    const count = stats.risk[level];
                    const pct =
                      stats.total > 0 ? (count / stats.total) * 100 : 0;
                    return (
                      <button
                        key={level}
                        onClick={() => router.push(`/claims?risk=${level}`)}
                        className="w-full grid grid-cols-[56px_1fr_28px] items-center gap-3 text-label group"
                        type="button"
                      >
                        <span
                          className={`text-right type-mono text-risk-${level} capitalize group-hover:opacity-80 transition-opacity`}
                        >
                          {level}
                        </span>
                        <div className="relative h-2 bg-muted overflow-hidden">
                          <div
                            className={`absolute inset-y-0 left-0 bg-risk-${level}`}
                            style={{ width: `${pct}%` }}
                          />
                        </div>
                        <span className="type-mono tabular-nums text-foreground text-right">
                          {count}
                        </span>
                      </button>
                    );
                  })}
                </div>
              </div>

              {/* Column 2 — Workflow status */}
              <div className="space-y-3">
                <p className="type-label text-muted-foreground">Workflow</p>
                <div className="space-y-2">
                  {(["new", "reviewed", "actioned"] as const).map((s) => (
                    <button
                      key={s}
                      onClick={() => router.push(`/claims?status=${s}`)}
                      type="button"
                      className="w-full flex items-center justify-between border border-border px-3 py-2.5 hover:bg-muted/50 transition-colors group"
                    >
                      <span className="text-caption text-muted-foreground capitalize group-hover:text-foreground transition-colors">
                        {s}
                      </span>
                      <span className="type-mono tabular-nums text-foreground font-medium">
                        {stats.status[s]}
                      </span>
                    </button>
                  ))}
                </div>
              </div>

              {/* Column 3 — Top 5 high-risk */}
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <p className="type-label text-muted-foreground">
                    Highest-risk claims
                  </p>
                  <Link
                    href="/claims?sort=riskScore&order=desc"
                    className="text-caption text-muted-foreground hover:text-foreground transition-colors"
                  >
                    View all →
                  </Link>
                </div>
                {topClaimsQuery.isLoading && (
                  <div className="space-y-2">
                    {Array.from({ length: 5 }).map((_, i) => (
                      <Skeleton key={i} className="h-8 w-full" />
                    ))}
                  </div>
                )}
                {topClaims.length > 0 && (
                  <div className="space-y-0 divide-y divide-border/60">
                    {topClaims.map((claim) => (
                      <Link
                        key={claim.claimId}
                        href={`/claims/${claim.claimId}`}
                        className="flex items-center gap-3 py-2 hover:bg-muted/30 -mx-2 px-2 transition-colors"
                      >
                        <RiskBar
                          score={claim.riskScore}
                          level={claim.riskLevel}
                        />
                        <span className="type-mono text-[11.5px] text-foreground truncate flex-1 min-w-0">
                          {claim.claimId}
                        </span>
                        {claim.analyzedAt && (
                          <span className="type-caption text-muted-foreground text-[10px] shrink-0">
                            {new Date(claim.analyzedAt).toLocaleDateString(
                              undefined,
                              {
                                month: "short",
                                day: "numeric",
                              },
                            )}
                          </span>
                        )}
                      </Link>
                    ))}
                  </div>
                )}
                {!topClaimsQuery.isLoading && topClaims.length === 0 && (
                  <p className="type-caption text-muted-foreground">
                    No analyzed claims yet.
                  </p>
                )}
              </div>
            </div>
          )}
        </section>

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
                aria-label="Claim ID"
              />
              <kbd className="absolute right-2 top-1/2 -translate-y-1/2 text-[10px] text-muted-foreground font-mono pointer-events-none">
                /
              </kbd>
            </div>
            <Button onClick={handleAnalyze} disabled={!claimId.trim()}>
              <MagnifyingGlass data-icon="inline-start" aria-hidden="true" />
              Analyze
            </Button>
          </div>
          <p className="type-caption text-muted-foreground">
            Press <kbd className="font-mono">/</kbd> to focus. Analyzes claim
            and opens detail view.
          </p>
        </section>
      </div>
    </AppShell>
  );
}

function StatusDot({ name, ok }: { name: string; ok: boolean }) {
  return (
    <span
      className="flex items-center gap-1.5"
      role="img"
      aria-label={`${name}: ${ok ? "connected" : "disconnected"}`}
    >
      <Circle
        size={8}
        weight="fill"
        className={ok ? "text-status-ok" : "text-status-err"}
        aria-hidden="true"
      />
      <span aria-hidden="true">{name}</span>
      {!ok && <span className="type-caption text-status-err">(offline)</span>}
    </span>
  );
}
