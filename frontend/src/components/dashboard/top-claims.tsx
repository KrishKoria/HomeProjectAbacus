import Link from "next/link";
import { RiskBar } from "@/components/risk-bar";
import { Skeleton } from "@/components/ui/skeleton";
import type { ClaimReview } from "@/lib/db/claims";

interface TopClaimsProps {
  isLoading: boolean;
  claims: ClaimReview[];
}

export function TopClaims({ isLoading, claims }: TopClaimsProps) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <p className="type-label text-muted-foreground">
          Highest-risk claims
        </p>
        <Link
          href="/claims?sort=riskScore&order=desc"
          className="type-caption text-muted-foreground hover:text-foreground transition-colors"
        >
          View all →
        </Link>
      </div>
      {isLoading && (
        <div role="status" aria-label="Loading top claims" className="space-y-2">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-8 w-full" />
          ))}
          <span className="sr-only">Loading top claims…</span>
        </div>
      )}
      {claims.length > 0 && (
        <div className="space-y-0 divide-y divide-border/60">
          {claims.map((claim) => (
            <Link
              key={claim.claimId}
              href={`/claims/${claim.claimId}`}
              className="flex items-center gap-3 py-2 hover:bg-muted/30 -mx-2 px-2 transition-colors"
            >
              <RiskBar
                score={claim.riskScore}
                level={claim.riskLevel}
              />
              <span className="type-mono type-caption text-foreground truncate flex-1 min-w-0">
                {claim.claimId}
              </span>
              {claim.analyzedAt && (
                <span className="type-caption text-muted-foreground shrink-0">
                  {formatDaysAgo(claim.analyzedAt)} in {claim.status}
                </span>
              )}
            </Link>
          ))}
        </div>
      )}
      {!isLoading && claims.length === 0 && (
        <p className="type-caption text-muted-foreground">
          No analyzed claims yet.
        </p>
      )}
    </div>
  );
}

function formatDaysAgo(date: Date | string): string {
  const ms = Date.now() - new Date(date).getTime();
  const days = Math.floor(ms / (1000 * 60 * 60 * 24));
  if (days === 0) return "<1d";
  return `${days}d`;
}
