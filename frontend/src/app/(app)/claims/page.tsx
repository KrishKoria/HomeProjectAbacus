"use client";

import { Suspense, useEffect, useRef, useState, useMemo } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
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
import { MagnifyingGlass, ArrowUp, ArrowDown } from "@phosphor-icons/react";
import type { ClaimReview } from "@/lib/db/claims";

type RiskLevel = "high" | "medium" | "low";
type StatusFilter = "all" | "new" | "reviewed" | "actioned";
type SortField = "riskScore" | "analyzedAt";
type SortDir = "asc" | "desc";
type AutoFillState =
  | { status: "idle" }
  | { status: "filling"; done: number; total: number }
  | { status: "done" };

const VALID_RISK = ["high", "medium", "low"] as const;
const VALID_STATUS = ["new", "reviewed", "actioned"] as const;

// Persists across navigations; resets on full page reload
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

function RiskBadge({ level }: { level: string }) {
  const normalized = level.toLowerCase() as RiskLevel;
  const cls = riskColors[normalized] ?? "bg-muted text-muted-foreground";
  return (
    <span className={`inline-flex items-center px-2 py-0.5 text-xs font-medium ${cls}`}>
      {normalized.charAt(0).toUpperCase() + normalized.slice(1)}
    </span>
  );
}

function StatusBadge({ status }: { status: string }) {
  const cls = statusColors[status] ?? "bg-muted text-muted-foreground";
  return (
    <span className={`inline-flex items-center px-2 py-0.5 text-xs font-medium ${cls}`}>
      {status.charAt(0).toUpperCase() + status.slice(1)}
    </span>
  );
}

function ScoreBar({ score }: { score: number }) {
  const pct = Math.round(score * 100);
  const level: RiskLevel = score >= 0.7 ? "high" : score >= 0.4 ? "medium" : "low";
  const barColor = level === "high" ? "bg-risk-high" : level === "medium" ? "bg-risk-medium" : "bg-risk-low";
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
  const queryClient = useQueryClient();
  const searchParams = useSearchParams();
  const searchRef = useRef<HTMLInputElement>(null);

  const [search, setSearch] = useState("");
  const [riskFilter, setRiskFilter] = useState<RiskLevel | "all">(() => {
    const r = searchParams.get("risk");
    return (VALID_RISK as readonly string[]).includes(r ?? "") ? (r as RiskLevel) : "all";
  });
  const [statusFilter, setStatusFilter] = useState<StatusFilter>(() => {
    const s = searchParams.get("status");
    return (VALID_STATUS as readonly string[]).includes(s ?? "") ? (s as StatusFilter) : "all";
  });
  const [sortField, setSortField] = useState<SortField>("riskScore");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [autoFill, setAutoFill] = useState<AutoFillState>({ status: "idle" });

  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ["claims"],
    queryFn: async () => {
      const res = await fetch("/api/claims");
      if (!res.ok) throw new Error("Failed to load claims");
      return res.json() as Promise<{ claims: ClaimReview[] }>;
    },
  });

  const samplesQuery = useQuery({
    queryKey: ["claim-samples"],
    queryFn: async () => {
      const res = await fetch("/api/claims/samples");
      if (!res.ok) throw new Error("Failed to fetch samples");
      return res.json() as Promise<{ claimIds: string[] }>;
    },
    staleTime: Infinity,
  });

  // Auto-fill: runs once per browser session when both queries have loaded
  useEffect(() => {
    if (autoFillStarted) return;
    if (!samplesQuery.data || !data) return;

    const analyzedSet = new Set(data.claims.map((c) => c.claimId));
    const toAnalyze = samplesQuery.data.claimIds.filter((id) => !analyzedSet.has(id));
    if (toAnalyze.length === 0) return;

    autoFillStarted = true;
    setAutoFill({ status: "filling", done: 0, total: toAnalyze.length });

    const CONCURRENCY = 3;
    let done = 0;
    (async () => {
      for (let i = 0; i < toAnalyze.length; i += CONCURRENCY) {
        const batch = toAnalyze.slice(i, i + CONCURRENCY);
        await Promise.all(
          batch.map(async (id) => {
            try {
              await fetch("/api/claims/analyze", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ claimId: id }),
              });
            } catch { /* skip individual failures */ }
            done++;
            setAutoFill({ status: "filling", done, total: toAnalyze.length });
            queryClient.invalidateQueries({ queryKey: ["claims"] });
          }),
        );
      }
      setAutoFill({ status: "done" });
      queryClient.invalidateQueries({ queryKey: ["claims"] });
    })();
  }, [samplesQuery.data, data, queryClient]);

  // Auto-dismiss "done" banner after 3s
  useEffect(() => {
    if (autoFill.status !== "done") return;
    const t = setTimeout(() => setAutoFill({ status: "idle" }), 3000);
    return () => clearTimeout(t);
  }, [autoFill.status]);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "/" && document.activeElement !== searchRef.current) {
        e.preventDefault();
        searchRef.current?.focus();
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  function toggleSort(field: SortField) {
    if (sortField === field) {
      setSortDir((d) => (d === "desc" ? "asc" : "desc"));
    } else {
      setSortField(field);
      setSortDir("desc");
    }
  }

  const filtered = useMemo(() => {
    let rows = data?.claims ?? [];
    if (search.trim()) {
      const q = search.trim().toLowerCase();
      rows = rows.filter((r) => r.claimId.toLowerCase().includes(q));
    }
    if (riskFilter !== "all") rows = rows.filter((r) => r.riskLevel.toLowerCase() === riskFilter);
    if (statusFilter !== "all") rows = rows.filter((r) => r.status === statusFilter);
    return [...rows].sort((a, b) => {
      const aVal = sortField === "riskScore" ? a.riskScore : new Date(a.analyzedAt).getTime();
      const bVal = sortField === "riskScore" ? b.riskScore : new Date(b.analyzedAt).getTime();
      return sortDir === "desc" ? bVal - aVal : aVal - bVal;
    });
  }, [data, search, riskFilter, statusFilter, sortField, sortDir]);

  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) return null;
    return sortDir === "desc"
      ? <ArrowDown className="inline size-3 ml-1" />
      : <ArrowUp className="inline size-3 ml-1" />;
  };

  return (
    <div className="p-6 space-y-5">
      <div className="flex items-baseline justify-between">
        <h1 className="type-headline">Claims</h1>
        {data && (
          <span className="type-caption text-muted-foreground">
            {filtered.length} of {data.claims.length}
          </span>
        )}
      </div>

      {/* Auto-fill progress banner */}
      {autoFill.status === "filling" && (
        <div className="flex items-center gap-3 border border-border px-4 py-2">
          <div className="flex-1 h-0.5 bg-muted overflow-hidden">
            <div
              className="h-full bg-primary transition-all duration-500"
              style={{ width: `${Math.round((autoFill.done / autoFill.total) * 100)}%` }}
            />
          </div>
          <span className="type-caption text-muted-foreground tabular-nums shrink-0">
            Analyzing {autoFill.done} / {autoFill.total}
          </span>
        </div>
      )}
      {autoFill.status === "done" && (
        <p className="type-caption text-muted-foreground">Queue ready.</p>
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
              onClick={() => setRiskFilter(level)}
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
              onClick={() => setStatusFilter(s)}
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

      {isLoading && <SkeletonTable />}

      {isError && (
        <div className="border border-border px-5 py-4 flex items-center gap-4">
          <p className="text-sm text-muted-foreground flex-1">
            Claims could not be loaded. Check your connection and try again.
          </p>
          <button
            onClick={() => refetch()}
            className="text-xs font-medium border border-border px-3 py-1.5 hover:bg-muted transition-colors"
          >
            Retry
          </button>
        </div>
      )}

      {!isLoading && !isError && filtered.length === 0 && (
        <div className="border border-border py-16 text-center">
          <p className="type-body text-muted-foreground mx-auto">
            {data && data.claims.length === 0 && autoFill.status === "idle"
              ? "No claims analyzed yet."
              : data && data.claims.length === 0
              ? "Analyzing claims from feature table…"
              : "No claims match the current filters."}
          </p>
        </div>
      )}

      {!isLoading && !isError && filtered.length > 0 && (
        <div className="border border-border">
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead className="type-label w-24">Risk</TableHead>
                <TableHead className="type-label">
                  <span>Claim ID</span>
                </TableHead>
                <TableHead className="type-label w-36">
                  <button
                    onClick={() => toggleSort("riskScore")}
                    className="flex items-center gap-1 hover:text-foreground transition-colors"
                  >
                    Score <SortIcon field="riskScore" />
                  </button>
                </TableHead>
                <TableHead className="type-label w-28">Status</TableHead>
                <TableHead className="type-label w-36">
                  <button
                    onClick={() => toggleSort("analyzedAt")}
                    className="flex items-center gap-1 hover:text-foreground transition-colors"
                  >
                    Analyzed <SortIcon field="analyzedAt" />
                  </button>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filtered.map((claim, i) => (
                <TableRow
                  key={claim.claimId}
                  className="cursor-pointer hover:bg-muted/60 animate-in fade-in slide-in-from-bottom-1"
                  style={{ animationDelay: `${Math.min(i * 20, 200)}ms`, animationFillMode: "backwards" }}
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
                    {new Date(claim.analyzedAt).toLocaleDateString(undefined, {
                      month: "short",
                      day: "numeric",
                      hour: "2-digit",
                      minute: "2-digit",
                    })}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
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
