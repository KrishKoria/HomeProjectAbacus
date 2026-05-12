"use client";

import { Suspense, useEffect, useRef, useState, useCallback } from "react";
import { useQuery } from "@tanstack/react-query";
import { useRouter, useSearchParams } from "next/navigation";
import { AppShell } from "@/components/app-shell";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Pagination,
  PaginationContent,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
} from "@/components/ui/pagination";
import { MagnifyingGlass, ArrowUp, ArrowDown } from "@phosphor-icons/react";
import { useAnalysisQueue } from "@/hooks/use-analysis-queue";
import type { PaginatedClaims } from "@/lib/db/claims";

type RiskLevel = "high" | "medium" | "low";
type StatusFilter = "all" | "new" | "reviewed" | "actioned";
type SortField = "riskScore" | "analyzedAt" | "claimId";
type SortDir = "asc" | "desc";

const VALID_RISK = ["high", "medium", "low"] as const;
const VALID_STATUS = ["new", "reviewed", "actioned"] as const;

let autoFillStarted = false;

const riskColors: Record<RiskLevel, string> = {
  high: "bg-risk-high-bg text-risk-high",
  medium: "bg-risk-medium-bg text-risk-medium",
  low: "bg-risk-low-bg text-risk-low",
};

const statusColors: Record<string, string> = {
  new: "bg-muted text-muted-foreground",
  reviewed: "bg-primary/10 text-primary",
  actioned: "bg-risk-low-bg text-risk-low",
};

function RiskBadge({ level }: { level: string | null }) {
  if (!level) {
    return (
      <span className="inline-flex items-center px-2 py-0.5 text-xs font-medium bg-muted text-muted-foreground opacity-50">
        —
      </span>
    );
  }
  const normalized = level.toLowerCase() as RiskLevel;
  const cls = riskColors[normalized] ?? "bg-muted text-muted-foreground";
  return (
    <span className={`inline-flex items-center px-2 py-0.5 text-xs font-medium ${cls}`}>
      {normalized.charAt(0).toUpperCase() + normalized.slice(1)}
    </span>
  );
}

function StatusBadge({ status }: { status: string }) {
  if (status === "new") {
    return (
      <span className="inline-flex items-center px-2 py-0.5 text-xs font-medium bg-muted text-muted-foreground opacity-50">
        Pending
      </span>
    );
  }
  const cls = statusColors[status] ?? "bg-muted text-muted-foreground";
  return (
    <span className={`inline-flex items-center px-2 py-0.5 text-xs font-medium ${cls}`}>
      {status.charAt(0).toUpperCase() + status.slice(1)}
    </span>
  );
}

function ScoreBar({ score }: { score: number | null }) {
  if (score === null) {
    return (
      <div className="flex items-center gap-2.5">
        <span className="type-mono tabular-nums w-8 text-right text-muted-foreground opacity-50">
          —
        </span>
        <div className="w-16 h-1 bg-muted overflow-hidden opacity-30" />
      </div>
    );
  }
  const pct = Math.round(score * 100);
  const level: RiskLevel = pct >= 70 ? "high" : pct >= 40 ? "medium" : "low";
  const barColor =
    level === "high"
      ? "bg-risk-high"
      : level === "medium"
        ? "bg-risk-medium"
        : "bg-risk-low";
  return (
    <div className="flex items-center gap-2.5">
      <span className="type-mono tabular-nums w-8 text-right">{pct}%</span>
      <div className="w-16 h-1 bg-muted overflow-hidden">
        <div className={`h-full ${barColor} transition-all duration-300`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function SkeletonTable() {
  return (
    <div className="border border-border">
      <div className="px-4 py-3 border-b border-border grid grid-cols-[120px_1fr_120px_100px_100px] gap-4">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-3 w-full" />
        ))}
      </div>
      {Array.from({ length: 8 }).map((_, i) => (
        <div
          key={i}
          className="px-4 py-3 border-b border-border last:border-b-0 grid grid-cols-[120px_1fr_120px_100px_100px] gap-4"
        >
          <Skeleton className="h-4 w-full" />
          <Skeleton className="h-4 w-2/3" />
          <Skeleton className="h-4 w-full" />
          <Skeleton className="h-4 w-3/4" />
          <Skeleton className="h-4 w-1/2" />
        </div>
      ))}
    </div>
  );
}

function ClaimsContent() {
  const router = useRouter();
  const searchRef = useRef<HTMLInputElement>(null);
  const searchParams = useSearchParams();

  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");

  const [riskFilter, setRiskFilter] = useState<RiskLevel | "all">(() => {
    const r = searchParams.get("risk");
    return (VALID_RISK as readonly string[]).includes(r ?? "")
      ? (r as RiskLevel)
      : "all";
  });

  const [statusFilter, setStatusFilter] = useState<StatusFilter>(() => {
    const s = searchParams.get("status");
    return (VALID_STATUS as readonly string[]).includes(s ?? "")
      ? (s as StatusFilter)
      : "all";
  });

  const [sortField, setSortField] = useState<SortField>("riskScore");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [page, setPage] = useState(1);

  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(search);
      setPage(1);
    }, 300);
    return () => clearTimeout(timer);
  }, [search]);

  const claimsQuery = useQuery({
    queryKey: ["claims", { page, search: debouncedSearch, risk: riskFilter, status: statusFilter, sort: sortField, order: sortDir }],
    queryFn: async () => {
      const params = new URLSearchParams({
        page: String(page),
        limit: "20",
        search: debouncedSearch,
        risk: riskFilter,
        status: statusFilter,
        sort: sortField,
        order: sortDir,
      });
      const res = await fetch(`/api/claims?${params}`);
      if (!res.ok) throw new Error("Failed to load claims");
      return res.json() as Promise<PaginatedClaims>;
    },
  });

  const statusesQuery = useQuery({
    queryKey: ["claim-statuses"],
    queryFn: async () => {
      const res = await fetch("/api/claims/statuses");
      if (!res.ok) throw new Error("Failed to fetch statuses");
      return res.json() as Promise<{
        statuses: { claimId: string; riskLevel: string | null; status: string; analyzedAt: string | null }[];
      }>;
    },
    staleTime: 5 * 60 * 1000,
  });

  const { enqueueBatch, progress } = useAnalysisQueue();

  useEffect(() => {
    if (autoFillStarted) return;
    if (!statusesQuery.data) return;

    const unanalyzed = statusesQuery.data.statuses
      .filter((s) => s.riskLevel === null)
      .map((s) => s.claimId);

    if (unanalyzed.length === 0) return;

    autoFillStarted = true;

    const foregroundCount = 20;
    const foreground = unanalyzed.slice(0, foregroundCount);
    const background = unanalyzed.slice(foregroundCount);

    if (foreground.length > 0) {
      enqueueBatch(foreground, 1);
    }
    if (background.length > 0) {
      setTimeout(() => {
        enqueueBatch(background, 0);
      }, 100);
    }
  }, [statusesQuery.data, enqueueBatch]);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "/" && document.activeElement !== searchRef.current && document.activeElement?.tagName !== "INPUT") {
        e.preventDefault();
        searchRef.current?.focus();
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const toggleSort = useCallback(
    (field: SortField) => {
      if (sortField === field) {
        setSortDir((d) => (d === "desc" ? "asc" : "desc"));
      } else {
        setSortField(field);
        setSortDir("desc");
      }
    },
    [sortField],
  );

  const getSortIcon = useCallback(
    (field: SortField) => {
      if (sortField !== field) return null;
      return sortDir === "desc" ? (
        <ArrowDown className="inline size-3 ml-1" />
      ) : (
        <ArrowUp className="inline size-3 ml-1" />
      );
    },
    [sortField, sortDir],
  );

  const data = claimsQuery.data;
  const claims = data?.claims ?? [];
  const total = data?.total ?? 0;
  const totalPages = data?.totalPages ?? 1;
  const analyzedCount = statusesQuery.data
    ? statusesQuery.data.statuses.filter((s) => s.riskLevel !== null).length
    : 0;
  const totalInDb = statusesQuery.data?.statuses.length ?? 0;
  const showingStart = claims.length > 0 ? (page - 1) * 20 + 1 : 0;
  const showingEnd = Math.min(page * 20, total);

  const isAnalyzing = progress.total > 0 && progress.completed < progress.total;

  return (
    <div className="p-6 space-y-5">
      <div className="flex items-baseline justify-between">
        <h1 className="type-headline">Claims</h1>
        <span className="type-caption text-muted-foreground">
          {totalInDb > 0
            ? `${analyzedCount} analyzed, ${totalInDb - analyzedCount} pending`
            : ""}
        </span>
      </div>

      {isAnalyzing && (
        <div className="flex items-center gap-3 border border-border px-4 py-2">
          <div className="flex-1 h-0.5 bg-muted overflow-hidden">
            <div
              className="h-full bg-primary transition-all duration-500"
              style={{
                width: `${Math.round((progress.completed / progress.total) * 100)}%`,
              }}
            />
          </div>
          <span className="type-caption text-muted-foreground tabular-nums shrink-0">
            Analyzing {progress.completed} / {progress.total}
          </span>
        </div>
      )}

      <div className="flex items-center gap-3 flex-wrap">
        <div className="relative">
          <MagnifyingGlass className="absolute left-2.5 top-1/2 -translate-y-1/2 size-3.5 text-muted-foreground pointer-events-none" />
          <Input
            ref={searchRef}
            placeholder="Search claim ID…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="pl-8 pr-7 w-56"
          />
          <kbd className="absolute right-2 top-1/2 -translate-y-1/2 text-[10px] text-muted-foreground font-mono pointer-events-none">
            /
          </kbd>
        </div>

        <div className="flex items-center gap-1" role="group" aria-label="Risk filter">
          {(["all", "high", "medium", "low"] as const).map((level) => (
            <button
              key={level}
              onClick={() => { setRiskFilter(level); setPage(1); }}
              className={`px-3 py-1.5 text-xs font-medium border transition-colors ${
                riskFilter === level
                  ? "bg-foreground text-background border-foreground"
                  : "border-border text-muted-foreground hover:text-foreground hover:border-foreground/40"
              }`}
            >
              {level === "all" ? "All risk" : level.charAt(0).toUpperCase() + level.slice(1)}
            </button>
          ))}
        </div>

        <div className="flex items-center gap-1" role="group" aria-label="Status filter">
          {(["all", "new", "reviewed", "actioned"] as const).map((s) => (
            <button
              key={s}
              onClick={() => { setStatusFilter(s); setPage(1); }}
              className={`px-3 py-1.5 text-xs font-medium border transition-colors ${
                statusFilter === s
                  ? "bg-foreground text-background border-foreground"
                  : "border-border text-muted-foreground hover:text-foreground hover:border-foreground/40"
              }`}
            >
              {s === "all" ? "All status" : s.charAt(0).toUpperCase() + s.slice(1)}
            </button>
          ))}
        </div>
      </div>

      {claimsQuery.isLoading && <SkeletonTable />}

      {claimsQuery.isError && (
        <div className="border border-border px-5 py-4 flex items-center gap-4">
          <p className="text-sm text-muted-foreground flex-1">
            Claims could not be loaded. Check your connection and try again.
          </p>
          <button
            onClick={() => claimsQuery.refetch()}
            className="text-xs font-medium border border-border px-3 py-1.5 hover:bg-muted transition-colors"
          >
            Retry
          </button>
        </div>
      )}

      {!claimsQuery.isLoading && !claimsQuery.isError && claims.length === 0 && (
        <div className="border border-border py-16 text-center">
          <p className="type-body text-muted-foreground mx-auto">
            {total === 0 && statusesQuery.data && statusesQuery.data.statuses.length > 0
              ? "Analyzing claims from feature table…"
              : total === 0
                ? "No claims found."
                : "No claims match the current filters."}
          </p>
        </div>
      )}

      {!claimsQuery.isLoading && !claimsQuery.isError && claims.length > 0 && (
        <>
          <div className="border border-border">
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="type-label w-24">Risk</TableHead>
                  <TableHead className="type-label">
                    <button
                      onClick={() => toggleSort("claimId")}
                      className="flex items-center gap-1 hover:text-foreground transition-colors"
                    >
                      Claim ID {getSortIcon("claimId")}
                    </button>
                  </TableHead>
                  <TableHead className="type-label w-36">
                    <button
                      onClick={() => toggleSort("riskScore")}
                      className="flex items-center gap-1 hover:text-foreground transition-colors"
                    >
                      Score {getSortIcon("riskScore")}
                    </button>
                  </TableHead>
                  <TableHead className="type-label w-28">Status</TableHead>
                  <TableHead className="type-label w-36">
                    <button
                      onClick={() => toggleSort("analyzedAt")}
                      className="flex items-center gap-1 hover:text-foreground transition-colors"
                    >
                      Analyzed {getSortIcon("analyzedAt")}
                    </button>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {claims.map((claim, i) => (
                  <TableRow
                    key={claim.claimId}
                    className={`cursor-pointer hover:bg-muted/60 animate-in fade-in slide-in-from-bottom-1 ${claim.riskLevel === null ? "opacity-50" : ""}`}
                    style={{
                      animationDelay: `${Math.min(i * 20, 200)}ms`,
                      animationFillMode: "backwards",
                    }}
                    onClick={() => router.push(`/claims/${claim.claimId}`)}
                  >
                    <TableCell>
                      <RiskBadge level={claim.riskLevel} />
                    </TableCell>
                    <TableCell className="type-mono font-medium">
                      {claim.claimId}
                    </TableCell>
                    <TableCell>
                      <ScoreBar score={claim.riskScore} />
                    </TableCell>
                    <TableCell>
                      <StatusBadge status={claim.status} />
                    </TableCell>
                    <TableCell className="type-caption text-muted-foreground">
                      {claim.analyzedAt
                        ? new Date(claim.analyzedAt).toLocaleDateString(undefined, {
                            month: "short",
                            day: "numeric",
                            hour: "2-digit",
                            minute: "2-digit",
                          })
                        : "—"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>

          <div className="flex items-center justify-between gap-4">
            <span className="type-caption whitespace-nowrap text-muted-foreground">
              Showing {showingStart}–{showingEnd} of {total}
            </span>

            <Pagination className="mx-0 w-auto shrink-0">
              <PaginationContent className="flex-nowrap">
                <PaginationItem>
                  <PaginationPrevious
                    onClick={() => page > 1 && setPage((p) => p - 1)}
                    className={page <= 1 ? "pointer-events-none opacity-50" : "cursor-pointer"}
                  />
                </PaginationItem>

                {Array.from({ length: totalPages }, (_, i) => i + 1)
                  .filter((p) => {
                    if (totalPages <= 7) return true;
                    if (p === 1 || p === totalPages) return true;
                    if (Math.abs(p - page) <= 1) return true;
                    return false;
                  })
                  .flatMap((p, idx, arr) => {
                    const showEllipsis = idx > 0 && p - arr[idx - 1] > 1;
                    const items = [];
                    if (showEllipsis) {
                      items.push(
                        <PaginationItem key={`ellipsis-${p}`}>
                          <span className="flex size-8 items-center justify-center text-muted-foreground">
                            …
                          </span>
                        </PaginationItem>
                      );
                    }
                    items.push(
                      <PaginationItem key={p}>
                        <PaginationLink
                          isActive={p === page}
                          onClick={() => setPage(p)}
                          className="cursor-pointer"
                        >
                          {p}
                        </PaginationLink>
                      </PaginationItem>
                    );
                    return items;
                  })}

                <PaginationItem>
                  <PaginationNext
                    onClick={() => page < totalPages && setPage((p) => p + 1)}
                    className={page >= totalPages ? "pointer-events-none opacity-50" : "cursor-pointer"}
                  />
                </PaginationItem>
              </PaginationContent>
            </Pagination>
          </div>
        </>
      )}
    </div>
  );
}

export default function ClaimsPage() {
  return (
    <AppShell>
      <Suspense fallback={<SkeletonTable />}>
        <ClaimsContent />
      </Suspense>
    </AppShell>
  );
}
