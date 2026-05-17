"use client";

import Link from "next/link";
import { type ReactNode } from "react";
import { RiskBar } from "@/components/risk-bar";
import { StatusBadge } from "@/components/claims/status-badge";
import {
  Pagination,
  PaginationContent,
  PaginationEllipsis,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
} from "@/components/ui/pagination";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

type SortField = "riskScore" | "analyzedAt" | "claimId";
type SortDir = "asc" | "desc";

interface ClaimRow {
  analyzedAt: Date | string | null;
  claimId: string;
  riskLevel: string | null;
  riskScore: number | null;
  status: string;
  topReason: string | null;
}

interface ClaimsTableProps {
  claims: ClaimRow[];
  effectiveFocusedRow: number;
  navigateToClaim: (claimId: string) => void;
  onToggleSort: (field: SortField) => void;
  page: number;
  sortDir: SortDir;
  sortField: SortField;
  statusCounts: {
    high: number;
    low: number;
    medium: number;
  };
  total: number;
  totalPages: number;
  buildHref: (updates: Record<string, string | null>) => string;
  getSortIcon: (field: SortField) => ReactNode;
}

export function ClaimsTable({
  claims,
  effectiveFocusedRow,
  navigateToClaim,
  onToggleSort,
  page,
  sortDir,
  sortField,
  statusCounts,
  total,
  totalPages,
  buildHref,
  getSortIcon,
}: ClaimsTableProps) {
  return (
    <>
      <div className="overflow-x-auto border border-border">
        <Table>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              <TableHead
                aria-sort={
                  sortField === "riskScore"
                    ? sortDir === "asc"
                      ? "ascending"
                      : "descending"
                    : "none"
                }
                className="type-label w-36"
              >
                <button
                  className="flex items-center gap-1 transition-colors hover:text-foreground"
                  onClick={() => onToggleSort("riskScore")}
                  type="button"
                >
                  Risk {getSortIcon("riskScore")}
                </button>
              </TableHead>
              <TableHead
                aria-sort={
                  sortField === "claimId"
                    ? sortDir === "asc"
                      ? "ascending"
                      : "descending"
                    : "none"
                }
                className="type-label"
              >
                <button
                  className="flex items-center gap-1 transition-colors hover:text-foreground"
                  onClick={() => onToggleSort("claimId")}
                  type="button"
                >
                  Claim ID {getSortIcon("claimId")}
                </button>
              </TableHead>
              <TableHead className="type-label">Finding</TableHead>
              <TableHead className="type-label w-28">Status</TableHead>
              <TableHead
                aria-sort={
                  sortField === "analyzedAt"
                    ? sortDir === "asc"
                      ? "ascending"
                      : "descending"
                    : "none"
                }
                className="type-label w-36"
              >
                <button
                  className="flex items-center gap-1 transition-colors hover:text-foreground"
                  onClick={() => onToggleSort("analyzedAt")}
                  type="button"
                >
                  Date {getSortIcon("analyzedAt")}
                </button>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {claims.map((claim, index) => (
              <TableRow
                key={claim.claimId}
                className={[
                  claim.riskLevel === null ? "opacity-50" : "",
                  effectiveFocusedRow === index
                    ? "outline outline-1 outline-ring -outline-offset-1"
                    : "",
                ]
                  .filter(Boolean)
                  .join(" ")}
                onClick={() => navigateToClaim(claim.claimId)}
                style={{ cursor: "pointer" }}
              >
                <TableCell>
                  <RiskBar
                    level={claim.riskLevel}
                    score={claim.riskScore}
                  />
                </TableCell>
                <TableCell className="type-mono font-medium">
                  <Link
                    className="underline-offset-4 transition-colors hover:text-foreground hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    href={`/claims/${claim.claimId}`}
                    onClick={(e) => {
                      e.stopPropagation();
                      e.preventDefault();
                      navigateToClaim(claim.claimId);
                    }}
                  >
                    {claim.claimId}
                  </Link>
                </TableCell>
                <TableCell className="type-caption text-muted-foreground truncate max-w-50">
                  {claim.topReason ?? "—"}
                </TableCell>
                <TableCell>
                  <StatusBadge status={claim.status} />
                </TableCell>
                <TableCell className="type-caption text-muted-foreground">
                  {claim.analyzedAt
                    ? new Date(claim.analyzedAt).toLocaleDateString(
                        undefined,
                        {
                          day: "numeric",
                          hour: "2-digit",
                          minute: "2-digit",
                          month: "short",
                        },
                      )
                    : "—"}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>

      <div className="h-10 flex items-center gap-6 px-1 type-caption text-muted-foreground border-t border-border/40">
        <span>
          <span className="tabular-nums text-foreground font-medium">
            {total}
          </span>{" "}
          claims
        </span>
        <span className="flex items-center gap-1.5">
          <span className="size-1.5 rounded-none bg-risk-high inline-block" />
          <span className="tabular-nums text-foreground font-medium">
            {statusCounts.high}
          </span>{" "}
          high
        </span>
        <span className="flex items-center gap-1.5">
          <span className="size-1.5 rounded-none bg-risk-medium inline-block" />
          <span className="tabular-nums text-foreground font-medium">
            {statusCounts.medium}
          </span>{" "}
          medium
        </span>
        <span className="flex items-center gap-1.5">
          <span className="size-1.5 rounded-none bg-risk-low inline-block" />
          <span className="tabular-nums text-foreground font-medium">
            {statusCounts.low}
          </span>{" "}
          low
        </span>
      </div>

      <div className="flex items-center justify-between gap-4">
        <span className="type-caption whitespace-nowrap text-muted-foreground">
          Showing {(page - 1) * 20 + 1}–{Math.min(page * 20, total)} of{" "}
          {total}
        </span>

        <Pagination className="mx-0 w-auto shrink-0">
          <PaginationContent className="flex-nowrap">
            <PaginationItem>
              <PaginationPrevious
                className={
                  page <= 1 ? "pointer-events-none opacity-50" : undefined
                }
                href={buildHref({
                  page: page > 1 ? String(page - 1) : null,
                })}
              />
            </PaginationItem>

            {Array.from({ length: totalPages }, (_, index) => index + 1)
              .filter((candidatePage) => {
                if (totalPages <= 7) return true;
                if (candidatePage === 1 || candidatePage === totalPages)
                  return true;
                return Math.abs(candidatePage - page) <= 1;
              })
              .flatMap((candidatePage, index, pages) => {
                const items: ReactNode[] = [];
                const shouldShowEllipsis =
                  index > 0 && candidatePage - pages[index - 1] > 1;

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
                className={
                  page >= totalPages
                    ? "pointer-events-none opacity-50"
                    : undefined
                }
                href={buildHref({
                  page:
                    page < totalPages
                      ? String(page + 1)
                      : String(totalPages),
                })}
              />
            </PaginationItem>
          </PaginationContent>
        </Pagination>
      </div>
    </>
  );
}
