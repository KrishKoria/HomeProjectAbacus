"use client";

import Link from "next/link";
import { useCallback, useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { AppShell } from "@/components/app-shell";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { ArrowRight, MagnifyingGlass } from "@phosphor-icons/react";
import { QueueBucket } from "@/components/dashboard/queue-bucket";
import { QueueStatusBucket } from "@/components/dashboard/queue-status-bucket";
import { RiskDistribution } from "@/components/dashboard/risk-distribution";
import { WorkflowStatus } from "@/components/dashboard/workflow-status";
import { TopClaims } from "@/components/dashboard/top-claims";
import type { ClaimReview, ClaimStats } from "@/lib/db/claims";

interface ClaimStatusRecord {
  analyzedAt: string | null;
  claimId: string;
  riskLevel: string | null;
  status: string;
}

interface WorkQueueData {
  highRiskNew: ClaimReview[];
  mediumRiskNew: ClaimReview[];
  missingAnalysis: ClaimStatusRecord[];
  recentlyDiscovered: ClaimStatusRecord[];
}

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

  const workQueueQuery = useQuery({
    queryKey: ["work-queue"],
    queryFn: async () => {
      const [highRes, mediumRes, statusesRes] = await Promise.all([
        fetch("/api/claims?risk=high&status=new&sort=riskScore&order=desc&limit=3"),
        fetch("/api/claims?risk=medium&status=new&sort=riskScore&order=desc&limit=3"),
        fetch("/api/claims/statuses"),
      ]);

      if (!highRes.ok || !mediumRes.ok || !statusesRes.ok) {
        throw new Error("Failed to load work queue");
      }

      const [highPayload, mediumPayload, statusesPayload] = await Promise.all([
        highRes.json() as Promise<{ claims: ClaimReview[] }>,
        mediumRes.json() as Promise<{ claims: ClaimReview[] }>,
        statusesRes.json() as Promise<{ statuses: ClaimStatusRecord[] }>,
      ]);

      const missingAnalysis = (statusesPayload.statuses ?? []).filter(
        (item) => item.riskLevel === null,
      );

      return {
        highRiskNew: highPayload.claims ?? [],
        mediumRiskNew: mediumPayload.claims ?? [],
        missingAnalysis,
        recentlyDiscovered: [...missingAnalysis].sort((a, b) =>
          b.claimId.localeCompare(a.claimId),
        ),
      } satisfies WorkQueueData;
    },
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
        <div role="status" aria-label="Loading dashboard" className="p-6 space-y-4">
          <Skeleton className="h-7 w-32" />
          <Skeleton className="h-10 w-full max-w-md" />
          <span className="sr-only">Loading dashboard…</span>
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
        </div>

        <section className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="type-title">Queue Overview</h2>
            <Link
              href="/claims"
              className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              View all <ArrowRight className="size-3" aria-hidden="true" />
            </Link>
          </div>

          {(statsQuery.isLoading || statsQuery.isPending) && (
            <div role="status" aria-label="Loading queue metrics" className="grid grid-cols-1 md:grid-cols-3 gap-6">
              {Array.from({ length: 3 }).map((_, i) => (
                <div key={i} className="space-y-3">
                  <Skeleton className="h-3 w-24" />
                  <Skeleton className="h-2 w-full" />
                  <Skeleton className="h-2 w-full" />
                  <Skeleton className="h-2 w-full" />
                </div>
              ))}
              <span className="sr-only">Loading queue metrics…</span>
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
              <RiskDistribution stats={stats} />
              <WorkflowStatus stats={stats} />
              <TopClaims
                isLoading={topClaimsQuery.isLoading}
                claims={topClaims}
              />
            </div>
          )}
        </section>

        <section className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="type-title">Work Queue</h2>
            <Link
              href="/claims?status=new"
              className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              Open queue <ArrowRight className="size-3" aria-hidden="true" />
            </Link>
          </div>

          {workQueueQuery.isLoading && (
            <div className="grid gap-4 md:grid-cols-2">
              {Array.from({ length: 4 }).map((_, index) => (
                <div key={index} className="border border-border p-4 space-y-3">
                  <Skeleton className="h-4 w-32" />
                  <Skeleton className="h-3 w-full" />
                  <Skeleton className="h-3 w-5/6" />
                </div>
              ))}
            </div>
          )}

          {workQueueQuery.data && (
            <div className="grid gap-4 md:grid-cols-2">
              <QueueBucket
                title="Highest-risk new claims"
                description="Prioritize the first review pass."
                href="/claims?risk=high&status=new&sort=riskScore&order=desc"
                claims={workQueueQuery.data.highRiskNew}
                emptyLabel="No new high-risk claims."
              />
              <QueueBucket
                title="Medium-risk unreviewed"
                description="Clear the next review tier."
                href="/claims?risk=medium&status=new&sort=riskScore&order=desc"
                claims={workQueueQuery.data.mediumRiskNew}
                emptyLabel="No medium-risk claims waiting."
              />
              <QueueStatusBucket
                title="Claims missing analysis"
                description="These claims have been discovered but not analyzed yet."
                href="/claims"
                claims={workQueueQuery.data.missingAnalysis}
                emptyLabel="No claims are waiting for analysis."
              />
              <QueueStatusBucket
                title="Recently discovered claims"
                description="Newest discovered claim IDs, using claim ID as a fallback sort."
                href="/claims"
                claims={workQueueQuery.data.recentlyDiscovered.slice(0, 3)}
                emptyLabel="No recently discovered claims."
              />
            </div>
          )}
        </section>

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
              <kbd className="absolute right-2 top-1/2 -translate-y-1/2 type-caption text-muted-foreground font-mono pointer-events-none">
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
