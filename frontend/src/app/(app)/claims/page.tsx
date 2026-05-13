"use client";

import Link from "next/link";
import { Suspense, useEffect, useRef, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { MagnifyingGlass, ArrowDown, ArrowUp } from "@phosphor-icons/react";
import { AppShell } from "@/components/app-shell";
import { RiskBar } from "@/components/risk-bar";
import { useAnalysisQueue } from "@/hooks/use-analysis-queue";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Pagination,
  PaginationContent,
  PaginationEllipsis,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
} from "@/components/ui/pagination";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { PaginatedClaims } from "@/lib/db/claims";

type RiskLevel = "high" | "medium" | "low";
type StatusFilter = "all" | "new" | "reviewed" | "actioned";
type SortField = "riskScore" | "analyzedAt" | "claimId";
type SortDir = "asc" | "desc";

const VALID_RISK = ["high", "medium", "low"] as const;
const VALID_STATUS = ["new", "reviewed", "actioned"] as const;
const VALID_SORT = ["riskScore", "analyzedAt", "claimId"] as const;
const VALID_ORDER = ["asc", "desc"] as const;
const AUTO_ANALYZE_LIMIT = 20;

const statusColors: Record<string, string> = {
  actioned: "bg-accent text-accent-foreground",
  new: "bg-muted text-muted-foreground",
  reviewed: "bg-primary/10 text-primary",
};

function StatusBadge({ status }: { status: string }) {
  const cls = statusColors[status] ?? "bg-muted text-muted-foreground";
  const label = status.charAt(0).toUpperCase() + status.slice(1);

  return (
    <span className={`inline-flex items-center px-2 py-0.5 text-xs font-medium ${cls}`}>
      {label}
    </span>
  );
}

function SkeletonTable() {
  return (
    <div className="border border-border overflow-x-auto">
      <div className="grid min-w-[540px] grid-cols-[minmax(120px,1fr)_minmax(120px,2fr)_minmax(120px,1fr)_minmax(100px,1fr)_minmax(100px,1fr)] gap-4 border-b border-border px-4 py-3">
        {Array.from({ length: 5 }).map((_, index) => (
          <Skeleton key={index} className="h-3 w-full" />
        ))}
      </div>
      {Array.from({ length: 8 }).map((_, index) => (
        <div
          key={index}
          className="grid min-w-[540px] grid-cols-[minmax(120px,1fr)_minmax(120px,2fr)_minmax(120px,1fr)_minmax(100px,1fr)_minmax(100px,1fr)] gap-4 border-b border-border px-4 py-3 last:border-b-0"
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

function SearchField({
  currentSearch,
  inputRef,
  onCommit,
}: {
  currentSearch: string;
  inputRef: { current: HTMLInputElement | null };
  onCommit: (value: string) => void;
}) {
  const [search, setSearch] = useState(currentSearch);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (timerRef.current) {
        clearTimeout(timerRef.current);
      }
    };
  }, []);

  return (
    <div className="relative">
      <MagnifyingGlass
        aria-hidden="true"
        className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-muted-foreground"
      />
      <Input
        ref={inputRef}
        aria-label="Search by claim ID"
        className="w-full pl-8 pr-7 sm:w-56"
        onChange={(event) => {
          const nextValue = event.target.value;
          setSearch(nextValue);

          if (timerRef.current) {
            clearTimeout(timerRef.current);
          }

          timerRef.current = setTimeout(() => {
            onCommit(nextValue);
          }, 300);
        }}
        placeholder="Search claim ID…"
        value={search}
      />
      <kbd className="pointer-events-none absolute top-1/2 right-2 -translate-y-1/2 font-mono text-[10px] text-muted-foreground">
        /
      </kbd>
    </div>
  );
}

function ClaimsContent() {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();
  const searchRef = useRef<HTMLInputElement>(null);
  const autoEnqueuedClaimIdsRef = useRef(new Set<string>());

  const currentSearch = searchParams.get("search") ?? "";
  const riskFilter = VALID_RISK.includes((searchParams.get("risk") ?? "") as RiskLevel)
    ? ((searchParams.get("risk") ?? "all") as RiskLevel)
    : "all";
  const statusFilter = (VALID_STATUS as readonly string[]).includes(searchParams.get("status") ?? "")
    ? ((searchParams.get("status") ?? "all") as StatusFilter)
    : "all";
  const sortField = VALID_SORT.includes((searchParams.get("sort") ?? "") as SortField)
    ? ((searchParams.get("sort") ?? "riskScore") as SortField)
    : "riskScore";
  const sortDir = VALID_ORDER.includes((searchParams.get("order") ?? "") as SortDir)
    ? ((searchParams.get("order") ?? "desc") as SortDir)
    : "desc";
  const page = Math.max(1, Number.parseInt(searchParams.get("page") ?? "1", 10) || 1);

  const buildHref = (updates: Record<string, string | null>) => {
    const params = new URLSearchParams(searchParams.toString());

    for (const [key, value] of Object.entries(updates)) {
      if (!value) {
        params.delete(key);
      } else {
        params.set(key, value);
      }
    }

    if (params.get("page") === "1") params.delete("page");
    if (params.get("risk") === "all") params.delete("risk");
    if (params.get("status") === "all") params.delete("status");
    if (params.get("sort") === "riskScore") params.delete("sort");
    if (params.get("order") === "desc") params.delete("order");
    if (!params.get("search")) params.delete("search");

    const query = params.toString();
    return query ? `${pathname}?${query}` : pathname;
  };

  const updateRoute = (updates: Record<string, string | null>) => {
    router.replace(buildHref(updates));
  };

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (
        event.key === "/" &&
        document.activeElement !== searchRef.current &&
        document.activeElement?.tagName !== "INPUT"
      ) {
        event.preventDefault();
        searchRef.current?.focus();
      }
    };

    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const claimsQuery = useQuery({
    queryKey: ["claims", { order: sortDir, page, risk: riskFilter, search: currentSearch, sort: sortField, status: statusFilter }],
    queryFn: async () => {
      const params = new URLSearchParams({
        limit: "20",
        order: sortDir,
        page: String(page),
        risk: riskFilter,
        search: currentSearch,
        sort: sortField,
        status: statusFilter,
      });
      const response = await fetch(`/api/claims?${params.toString()}`);
      if (!response.ok) throw new Error("Failed to load claims");
      return response.json() as Promise<PaginatedClaims>;
    },
  });

  const statusesQuery = useQuery({
    queryKey: ["claim-statuses"],
    queryFn: async () => {
      const response = await fetch("/api/claims/statuses");
      if (!response.ok) throw new Error("Failed to fetch statuses");
      return response.json() as Promise<{
        statuses: { analyzedAt: string | null; claimId: string; riskLevel: string | null; status: string }[];
      }>;
    },
    staleTime: 5 * 60 * 1000,
  });

  const { enqueueBatch, isProcessing, progress } = useAnalysisQueue();
  const claims = claimsQuery.data?.claims ?? [];
  const total = claimsQuery.data?.total ?? 0;
  const totalPages = claimsQuery.data?.totalPages ?? 1;
  const statuses = statusesQuery.data?.statuses ?? [];
  const statusesByClaimId = new Map(statuses.map((status) => [status.claimId, status]));
  const analyzedCount = statuses.filter((status) => status.riskLevel !== null).length;
  const totalInDb = statuses.length;
  const allUnanalyzedClaimIds = statuses
    .filter((status) => status.riskLevel === null)
    .map((status) => status.claimId);
  const visibleUnanalyzedClaimIds = claims
    .map((claim) => claim.claimId)
    .filter((claimId) => statusesByClaimId.get(claimId)?.riskLevel === null);
  const autoAnalyzeSeedClaimIds =
    visibleUnanalyzedClaimIds.length > 0
      ? visibleUnanalyzedClaimIds
      : allUnanalyzedClaimIds.slice(0, AUTO_ANALYZE_LIMIT);
  const remainingUnanalyzedClaimIds = allUnanalyzedClaimIds.filter(
    (claimId) => !autoAnalyzeSeedClaimIds.includes(claimId),
  );
  const showingStart = claims.length > 0 ? (page - 1) * 20 + 1 : 0;
  const showingEnd = Math.min(page * 20, total);
  const hasQueueActivity = progress.total > 0;

  useEffect(() => {
    const autoAnalyzeClaimIds = autoAnalyzeSeedClaimIds.filter(
      (claimId) => !autoEnqueuedClaimIdsRef.current.has(claimId),
    );

    if (autoAnalyzeClaimIds.length === 0) return;

    for (const claimId of autoAnalyzeClaimIds) {
      autoEnqueuedClaimIdsRef.current.add(claimId);
    }

    enqueueBatch(autoAnalyzeClaimIds, 1);
  }, [autoAnalyzeSeedClaimIds, enqueueBatch]);

  const enqueueRemainingClaims = () => {
    if (remainingUnanalyzedClaimIds.length === 0) return;

    for (const claimId of remainingUnanalyzedClaimIds) {
      autoEnqueuedClaimIdsRef.current.add(claimId);
    }

    enqueueBatch(remainingUnanalyzedClaimIds, 0);
  };

  const toggleSort = (field: SortField) => {
    if (sortField === field) {
      updateRoute({ order: sortDir === "desc" ? "asc" : "desc" });
      return;
    }

    updateRoute({
      order: "desc",
      sort: field,
    });
  };

  const getSortIcon = (field: SortField) => {
    if (sortField !== field) return null;
    return sortDir === "desc" ? (
      <ArrowDown className="ml-1 inline size-3" />
    ) : (
      <ArrowUp className="ml-1 inline size-3" />
    );
  };

  return (
    <div className="space-y-5 p-6">
      <div className="flex items-baseline justify-between">
        <h1 className="type-headline">Claims</h1>
        <span className="type-caption text-muted-foreground">
          {totalInDb > 0 ? `${analyzedCount} analyzed, ${totalInDb - analyzedCount} pending` : ""}
        </span>
      </div>

      {hasQueueActivity && (
        <div className="space-y-2 border border-border px-4 py-3">
          <div className="flex items-center gap-3">
            <div
              aria-label="Analysis progress"
              aria-valuemax={100}
              aria-valuemin={0}
              aria-valuenow={progress.total === 0 ? 0 : Math.round(((progress.completed + progress.failed) / progress.total) * 100)}
              className="h-0.5 flex-1 overflow-hidden bg-muted"
              role="progressbar"
            >
              <div
                className="h-full bg-primary transition-all duration-500"
                style={{
                  width: `${progress.total === 0 ? 0 : Math.round(((progress.completed + progress.failed) / progress.total) * 100)}%`,
                }}
              />
            </div>
            <span className="type-caption shrink-0 tabular-nums text-muted-foreground">
              {isProcessing ? "Analyzing" : "Queued"} {progress.completed + progress.failed} / {progress.total}
            </span>
          </div>
          {(progress.failed > 0 || progress.current) && (
            <div className="flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
              {progress.current && <span>Current: {progress.current}</span>}
              {progress.failed > 0 && <span>{progress.failed} failed</span>}
            </div>
          )}
        </div>
      )}

      <div className="flex flex-wrap items-center gap-4">
        <SearchField
          key={currentSearch}
          currentSearch={currentSearch}
          inputRef={searchRef}
          onCommit={(value) =>
            updateRoute({
              page: null,
              search: value.trim() ? value.trim() : null,
            })
          }
        />

        <div
          aria-label="Filter by risk level"
          className="flex items-center border border-border"
          role="group"
        >
          {(["all", "high", "medium", "low"] as const).map((level, i) => (
            <button
              key={level}
              aria-pressed={riskFilter === level}
              className={`h-8 px-2.5 text-[12px] font-medium transition-colors ${
                i > 0 ? "border-l border-border" : ""
              } ${
                riskFilter === level
                  ? "bg-foreground text-background"
                  : "text-muted-foreground hover:text-foreground hover:bg-muted"
              }`}
              onClick={() =>
                updateRoute({
                  page: null,
                  risk: level === "all" ? null : level,
                })
              }
              type="button"
            >
              {level === "all" ? "All" : level.charAt(0).toUpperCase() + level.slice(1)}
            </button>
          ))}
        </div>

        <div
          aria-label="Filter by status"
          className="flex items-center border border-border"
          role="group"
        >
          {(["all", "new", "reviewed", "actioned"] as const).map((status, i) => (
            <button
              key={status}
              aria-pressed={statusFilter === status}
              className={`h-8 px-2.5 text-[12px] font-medium transition-colors ${
                i > 0 ? "border-l border-border" : ""
              } ${
                statusFilter === status
                  ? "bg-foreground text-background"
                  : "text-muted-foreground hover:text-foreground hover:bg-muted"
              }`}
              onClick={() =>
                updateRoute({
                  page: null,
                  status: status === "all" ? null : status,
                })
              }
              type="button"
            >
              {status === "all" ? "All" : status.charAt(0).toUpperCase() + status.slice(1)}
            </button>
          ))}
        </div>

        <div className="ml-auto flex items-center gap-2">
          <Button
            disabled={remainingUnanalyzedClaimIds.length === 0}
            onClick={enqueueRemainingClaims}
            size="sm"
            variant="outline"
          >
            Analyze remaining
          </Button>
          {remainingUnanalyzedClaimIds.length > 0 && (
            <span className="type-caption text-muted-foreground">
              {remainingUnanalyzedClaimIds.length} waiting
            </span>
          )}
        </div>
      </div>

      {claimsQuery.isLoading && <SkeletonTable />}

      {claimsQuery.isError && (
        <div className="flex items-center gap-4 border border-border px-5 py-4" role="alert">
          <p className="type-body flex-1 text-muted-foreground">
            Claims could not be loaded. Check your connection and try again.
          </p>
          <Button onClick={() => claimsQuery.refetch()} size="sm" variant="outline">
            Retry
          </Button>
        </div>
      )}

      {!claimsQuery.isLoading && !claimsQuery.isError && claims.length === 0 && (
        <div className="border border-border py-16 text-center" role="status">
          <p className="type-body mx-auto text-muted-foreground">
            {total === 0 && statuses.length > 0
              ? "Analyzing claims from feature table…"
              : total === 0
                ? "No claims found."
                : "No claims match the current filters."}
          </p>
        </div>
      )}

      {!claimsQuery.isLoading && !claimsQuery.isError && claims.length > 0 && (
        <>
          <div className="overflow-x-auto border border-border">
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead
                    aria-sort={sortField === "riskScore" ? (sortDir === "asc" ? "ascending" : "descending") : "none"}
                    className="type-label w-36"
                  >
                    <button
                      className="flex items-center gap-1 transition-colors hover:text-foreground"
                      onClick={() => toggleSort("riskScore")}
                      type="button"
                    >
                      Risk {getSortIcon("riskScore")}
                    </button>
                  </TableHead>
                  <TableHead
                    aria-sort={sortField === "claimId" ? (sortDir === "asc" ? "ascending" : "descending") : "none"}
                    className="type-label"
                  >
                    <button
                      className="flex items-center gap-1 transition-colors hover:text-foreground"
                      onClick={() => toggleSort("claimId")}
                      type="button"
                    >
                      Claim ID {getSortIcon("claimId")}
                    </button>
                  </TableHead>
                  <TableHead className="type-label">Finding</TableHead>
                  <TableHead className="type-label w-28">Status</TableHead>
                  <TableHead
                    aria-sort={sortField === "analyzedAt" ? (sortDir === "asc" ? "ascending" : "descending") : "none"}
                    className="type-label w-36"
                  >
                    <button
                      className="flex items-center gap-1 transition-colors hover:text-foreground"
                      onClick={() => toggleSort("analyzedAt")}
                      type="button"
                    >
                      Date {getSortIcon("analyzedAt")}
                    </button>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {claims.map((claim) => (
                  <TableRow
                    key={claim.claimId}
                    className={claim.riskLevel === null ? "opacity-50" : undefined}
                  >
                    <TableCell>
                      <RiskBar level={claim.riskLevel} score={claim.riskScore} />
                    </TableCell>
                    <TableCell className="type-mono font-medium">
                      <Link
                        className="underline-offset-4 transition-colors hover:text-foreground hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                        href={`/claims/${claim.claimId}`}
                      >
                        {claim.claimId}
                      </Link>
                    </TableCell>
                    <TableCell className="type-caption text-muted-foreground truncate max-w-[200px]">
                      {claim.topReason ?? "—"}
                    </TableCell>
                    <TableCell>
                      <StatusBadge status={claim.status} />
                    </TableCell>
                    <TableCell className="type-caption text-muted-foreground">
                      {claim.analyzedAt
                        ? new Date(claim.analyzedAt).toLocaleDateString(undefined, {
                            day: "numeric",
                            hour: "2-digit",
                            minute: "2-digit",
                            month: "short",
                          })
                        : "—"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>

          <div className="h-10 flex items-center gap-6 px-1 text-[11.5px] text-muted-foreground border-t border-border/40">
            <span>
              <span className="tabular-nums text-foreground font-medium">{total}</span> claims
            </span>
            <span className="flex items-center gap-1.5">
              <span className="size-1.5 rounded-none bg-risk-high inline-block" />
              <span className="tabular-nums text-foreground font-medium">
                {statuses.filter((s) => s.riskLevel === "high").length}
              </span>{" "}
              high
            </span>
            <span className="flex items-center gap-1.5">
              <span className="size-1.5 rounded-none bg-risk-medium inline-block" />
              <span className="tabular-nums text-foreground font-medium">
                {statuses.filter((s) => s.riskLevel === "medium").length}
              </span>{" "}
              medium
            </span>
            <span className="flex items-center gap-1.5">
              <span className="size-1.5 rounded-none bg-risk-low inline-block" />
              <span className="tabular-nums text-foreground font-medium">
                {statuses.filter((s) => s.riskLevel === "low").length}
              </span>{" "}
              low
            </span>
          </div>

          <div className="flex items-center justify-between gap-4">
            <span className="type-caption whitespace-nowrap text-muted-foreground">
              Showing {showingStart}–{showingEnd} of {total}
            </span>

            <Pagination className="mx-0 w-auto shrink-0">
              <PaginationContent className="flex-nowrap">
                <PaginationItem>
                  <PaginationPrevious
                    className={page <= 1 ? "pointer-events-none opacity-50" : undefined}
                    href={buildHref({ page: page > 1 ? String(page - 1) : null })}
                  />
                </PaginationItem>

                {Array.from({ length: totalPages }, (_, index) => index + 1)
                  .filter((candidatePage) => {
                    if (totalPages <= 7) return true;
                    if (candidatePage === 1 || candidatePage === totalPages) return true;
                    return Math.abs(candidatePage - page) <= 1;
                  })
                  .flatMap((candidatePage, index, pages) => {
                    const items: ReactNode[] = [];
                    const shouldShowEllipsis = index > 0 && candidatePage - pages[index - 1] > 1;

                    if (shouldShowEllipsis) {
                      items.push(
                        <PaginationItem key={`ellipsis-${candidatePage}`}>
                          <PaginationEllipsis className="text-muted-foreground" />
                        </PaginationItem>,
                      );
                    }

                    items.push(
                      <PaginationItem key={candidatePage}>
                        <PaginationLink
                          href={buildHref({ page: String(candidatePage) })}
                          isActive={candidatePage === page}
                        >
                          {candidatePage}
                        </PaginationLink>
                      </PaginationItem>,
                    );

                    return items;
                  })}

                <PaginationItem>
                  <PaginationNext
                    className={page >= totalPages ? "pointer-events-none opacity-50" : undefined}
                    href={buildHref({ page: page < totalPages ? String(page + 1) : String(totalPages) })}
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
