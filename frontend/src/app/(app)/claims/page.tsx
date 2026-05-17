"use client";

import {
  Suspense,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useQuery } from "@tanstack/react-query";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { ArrowDown, ArrowUp } from "@phosphor-icons/react";
import { AppShell } from "@/components/app-shell";
import { QueueHelpPanel } from "@/components/queue-help-panel";
import { useAnalysisQueue } from "@/hooks/use-analysis-queue";
import { Button } from "@/components/ui/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { ClaimsTableSkeleton } from "@/components/claims/claims-table-skeleton";
import { SearchField } from "@/components/claims/search-field";
import { ClaimsFilterBar } from "@/components/claims/claims-filter-bar";
import { AnalysisProgressBar } from "@/components/claims/analysis-progress-bar";
import { ClaimsTable } from "@/components/claims/claims-table";
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

/** sessionStorage key scoped to the current URL for scroll restore */
function scrollKey(pathname: string, search: string) {
  return `scroll:${pathname}${search}`;
}

function ClaimsContent() {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();
  const searchRef = useRef<HTMLInputElement>(null);
  const autoEnqueuedClaimIdsRef = useRef(new Set<string>());
  const lastHandledSyncAtRef = useRef(0);

  const [analyzePopoverOpen, setAnalyzePopoverOpen] = useState(false);
  const [focusedRowIndex, setFocusedRowIndex] = useState(-1);

  const currentSearch = searchParams.get("search") ?? "";
  const riskFilter = VALID_RISK.includes(
    (searchParams.get("risk") ?? "") as RiskLevel,
  )
    ? ((searchParams.get("risk") ?? "all") as RiskLevel)
    : "all";
  const statusFilter = (VALID_STATUS as readonly string[]).includes(
    searchParams.get("status") ?? "",
  )
    ? ((searchParams.get("status") ?? "all") as StatusFilter)
    : "all";
  const sortField = VALID_SORT.includes(
    (searchParams.get("sort") ?? "") as SortField,
  )
    ? ((searchParams.get("sort") ?? "riskScore") as SortField)
    : "riskScore";
  const sortDir = VALID_ORDER.includes(
    (searchParams.get("order") ?? "") as SortDir,
  )
    ? ((searchParams.get("order") ?? "desc") as SortDir)
    : "desc";
  const page = Math.max(
    1,
    Number.parseInt(searchParams.get("page") ?? "1", 10) || 1,
  );

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

  useLayoutEffect(() => {
    const key = scrollKey(pathname, window.location.search);
    const raw = sessionStorage.getItem(key);
    if (raw !== null) {
      const top = Number(raw);
      sessionStorage.removeItem(key);
      window.scrollTo({ top, behavior: "instant" });
    }
  }, [pathname]);

  const claimsQuery = useQuery({
    queryKey: [
      "claims",
      {
        order: sortDir,
        page,
        risk: riskFilter,
        search: currentSearch,
        sort: sortField,
        status: statusFilter,
      },
    ],
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
        statuses: {
          analyzedAt: string | null;
          claimId: string;
          riskLevel: string | null;
          status: string;
        }[];
      }>;
    },
    staleTime: 5 * 60 * 1000,
  });

  const { enqueueBatch, isProcessing, progress } = useAnalysisQueue();

  const syncQuery = useQuery({
    queryKey: ["claim-sync"],
    queryFn: async () => {
      const response = await fetch("/api/claims/sync", {
        method: "POST",
      });

      const payload = (await response
        .json()
        .catch(() => ({ error: "Failed to sync claim IDs" }))) as {
        error?: string;
      };

      if (!response.ok) {
        throw new Error(payload.error ?? "Failed to sync claim IDs");
      }

      return payload;
    },
    staleTime: 60_000,
    refetchOnMount: true,
    refetchOnWindowFocus: false,
    retry: false,
  });

  const claims = useMemo(
    () => claimsQuery.data?.claims ?? [],
    [claimsQuery.data],
  );
  const total = claimsQuery.data?.total ?? 0;
  const totalPages = claimsQuery.data?.totalPages ?? 1;
  const statuses = useMemo(
    () => statusesQuery.data?.statuses ?? [],
    [statusesQuery.data],
  );

  const navigateToClaim = useCallback(
    (claimId: string) => {
      const key = scrollKey(pathname, window.location.search);
      sessionStorage.setItem(key, String(window.scrollY));
      router.push(`/claims/${claimId}`);
    },
    [pathname, router],
  );

  const effectiveFocusedRow =
    focusedRowIndex >= 0 && focusedRowIndex < claims.length
      ? focusedRowIndex
      : -1;

  const claimsSnapshotRef = useRef<Array<{ claimId: string }>>([]);
  const focusedRowIndexRef = useRef(-1);

  useLayoutEffect(() => {
    claimsSnapshotRef.current = claims;
    focusedRowIndexRef.current = effectiveFocusedRow;
  });

  useEffect(() => {
    const isInputFocused = () => {
      const tag = document.activeElement?.tagName;
      return tag === "INPUT" || tag === "TEXTAREA";
    };

    const onKey = (event: KeyboardEvent) => {
      if (
        event.key === "/" &&
        document.activeElement !== searchRef.current &&
        !isInputFocused()
      ) {
        event.preventDefault();
        searchRef.current?.focus();
        return;
      }

      if (isInputFocused()) return;

      if (event.key === "j") {
        event.preventDefault();
        const len = claimsSnapshotRef.current.length;
        if (len === 0) return;
        setFocusedRowIndex((prev) => Math.min(prev + 1, len - 1));
        return;
      }

      if (event.key === "k") {
        event.preventDefault();
        const len = claimsSnapshotRef.current.length;
        if (len === 0) return;
        setFocusedRowIndex((prev) => Math.max(prev - 1, 0));
        return;
      }

      if (event.key === "Enter") {
        const idx = focusedRowIndexRef.current;
        const snapshot = claimsSnapshotRef.current;
        if (idx >= 0 && idx < snapshot.length) {
          event.preventDefault();
          navigateToClaim(snapshot[idx].claimId);
        }
      }
    };

    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [navigateToClaim]);

  const { statusesByClaimId, analyzedCount, totalInDb, allUnanalyzedClaimIds } =
    useMemo(() => {
      const statusesByClaimId = new Map(
        statuses.map((status) => [status.claimId, status]),
      );
      const analyzedCount = statuses.filter(
        (status) => status.riskLevel !== null,
      ).length;
      const totalInDb = statuses.length;
      const allUnanalyzedClaimIds = statuses
        .filter((status) => status.riskLevel === null)
        .map((status) => status.claimId);
      return {
        statusesByClaimId,
        analyzedCount,
        totalInDb,
        allUnanalyzedClaimIds,
      };
    }, [statuses]);

  const { autoAnalyzeSeedClaimIds, remainingUnanalyzedClaimIds } =
    useMemo(() => {
      const visibleUnanalyzedClaimIds = claims
        .map((claim) => claim.claimId)
        .filter(
          (claimId) => statusesByClaimId.get(claimId)?.riskLevel === null,
        );
      const autoAnalyzeSeedClaimIds =
        visibleUnanalyzedClaimIds.length > 0
          ? visibleUnanalyzedClaimIds
          : allUnanalyzedClaimIds.slice(0, AUTO_ANALYZE_LIMIT);
      const remainingUnanalyzedClaimIds = allUnanalyzedClaimIds.filter(
        (claimId) => !autoAnalyzeSeedClaimIds.includes(claimId),
      );
      return { autoAnalyzeSeedClaimIds, remainingUnanalyzedClaimIds };
    }, [claims, statusesByClaimId, allUnanalyzedClaimIds]);

  useEffect(() => {
    if (!syncQuery.isSuccess || syncQuery.dataUpdatedAt === 0) return;
    if (lastHandledSyncAtRef.current === syncQuery.dataUpdatedAt) return;

    lastHandledSyncAtRef.current = syncQuery.dataUpdatedAt;
    void Promise.all([claimsQuery.refetch(), statusesQuery.refetch()]);
  }, [
    claimsQuery,
    statusesQuery,
    syncQuery.dataUpdatedAt,
    syncQuery.isSuccess,
  ]);

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
    setAnalyzePopoverOpen(false);
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

  const pendingCount = remainingUnanalyzedClaimIds.length;
  const analyzeBtnLabel =
    pendingCount > 0
      ? `Analyze ${pendingCount} pending`
      : "Analyze all pending";
  const estimatedSeconds = Math.ceil(pendingCount * 0.3);

  const statusCounts = useMemo(
    () => ({
      high: statuses.filter((s) => s.riskLevel === "high").length,
      low: statuses.filter((s) => s.riskLevel === "low").length,
      medium: statuses.filter((s) => s.riskLevel === "medium").length,
    }),
    [statuses],
  );

  return (
    <div className="space-y-5 p-6">
      <div className="flex items-center justify-between">
        <h1 className="type-headline">Claims</h1>
        <div className="flex items-center gap-2">
          <span className="type-caption text-muted-foreground">
            {totalInDb > 0
              ? `${analyzedCount} analyzed, ${totalInDb - analyzedCount} pending`
              : ""}
          </span>
          <QueueHelpPanel />
        </div>
      </div>

      <AnalysisProgressBar isProcessing={isProcessing} progress={progress} />

      {syncQuery.isError && (
        <div
          className="flex items-center gap-4 border border-border px-5 py-4"
          role="alert"
        >
          <p className="type-body flex-1 text-muted-foreground">
            {(syncQuery.error as Error).message}
          </p>
          <Button
            onClick={() => syncQuery.refetch()}
            size="sm"
            variant="outline"
          >
            Retry sync
          </Button>
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

        <ClaimsFilterBar
          riskFilter={riskFilter}
          statusFilter={statusFilter}
          onRiskChange={(risk) =>
            updateRoute({ page: null, risk })
          }
          onStatusChange={(status) =>
            updateRoute({ page: null, status })
          }
        />

        <div className="ml-auto flex items-center gap-2">
          {pendingCount > 50 ? (
            <Popover
              open={analyzePopoverOpen}
              onOpenChange={setAnalyzePopoverOpen}
            >
              <PopoverTrigger
                render={
                  <Button
                    disabled={pendingCount === 0}
                    size="sm"
                    variant="outline"
                  />
                }
              >
                {analyzeBtnLabel}
              </PopoverTrigger>
              <PopoverContent align="end" side="bottom" className="w-80 p-4">
                <p className="type-body mb-3">
                  Will analyze {pendingCount} claims. Estimated time: ~
                  {estimatedSeconds}s. Fires {pendingCount} model serving
                  requests.
                </p>
                <div className="flex justify-end gap-2">
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => setAnalyzePopoverOpen(false)}
                  >
                    Cancel
                  </Button>
                  <Button size="sm" onClick={enqueueRemainingClaims}>
                    Start analysis
                  </Button>
                </div>
              </PopoverContent>
            </Popover>
          ) : (
            <Button
              disabled={pendingCount === 0}
              onClick={enqueueRemainingClaims}
              size="sm"
              variant="outline"
            >
              {analyzeBtnLabel}
            </Button>
          )}
        </div>
      </div>

      {claimsQuery.isLoading && <ClaimsTableSkeleton />}

      {claimsQuery.isError && (
        <div
          className="flex items-center gap-4 border border-border px-5 py-4"
          role="alert"
        >
          <p className="type-body flex-1 text-muted-foreground">
            Claims could not be loaded. Check your connection and try again.
          </p>
          <Button
            onClick={() => claimsQuery.refetch()}
            size="sm"
            variant="outline"
          >
            Retry
          </Button>
        </div>
      )}

      {!claimsQuery.isLoading &&
        !claimsQuery.isError &&
        claims.length === 0 && (
          <div className="border border-border py-16 text-center" role="status">
            {total === 0 && statuses.length > 0 ? (
              <p className="type-body mx-auto text-muted-foreground">
                Analyzing claims from feature table&hellip;{" "}
                {progress.total > 0
                  ? `(${progress.completed}/${progress.total} analyzed)`
                  : "Queueing up…"}
              </p>
            ) : total === 0 ? (
              <>
                <p className="type-body mx-auto max-w-md text-muted-foreground">
                  No claims in your queue yet. Once claims land in the feature
                  table, they&apos;ll be analyzed automatically and appear here
                  sorted by denial risk.
                </p>
                <p className="type-caption mx-auto mt-2 text-muted-foreground">
                  Check back in a few minutes, or visit the dashboard for system
                  status.
                </p>
              </>
            ) : (
              <p className="type-body mx-auto text-muted-foreground">
                No claims match the current filters.
              </p>
            )}
          </div>
        )}

      {!claimsQuery.isLoading && !claimsQuery.isError && claims.length > 0 && (
        <ClaimsTable
          claims={claims}
          effectiveFocusedRow={effectiveFocusedRow}
          navigateToClaim={navigateToClaim}
          onToggleSort={toggleSort}
          page={page}
          sortDir={sortDir}
          sortField={sortField}
          statusCounts={statusCounts}
          total={total}
          totalPages={totalPages}
          buildHref={buildHref}
          getSortIcon={getSortIcon}
        />
      )}
    </div>
  );
}

export default function ClaimsPage() {
  return (
    <AppShell>
      <Suspense fallback={<ClaimsTableSkeleton />}>
        <ClaimsContent />
      </Suspense>
    </AppShell>
  );
}
