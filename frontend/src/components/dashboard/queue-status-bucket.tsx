import Link from "next/link";

interface ClaimStatusRecord {
  analyzedAt: string | null;
  claimId: string;
  riskLevel: string | null;
  status: string;
}

export function QueueStatusBucket({
  claims,
  description,
  emptyLabel,
  href,
  title,
}: {
  claims: ClaimStatusRecord[];
  description: string;
  emptyLabel: string;
  href: string;
  title: string;
}) {
  return (
    <div className="border border-border p-4 space-y-3">
      <div className="flex items-center justify-between gap-2">
        <div>
          <h3 className="type-label text-foreground">{title}</h3>
          <p className="type-caption text-muted-foreground">{description}</p>
        </div>
        <Link href={href} className="type-caption text-muted-foreground hover:text-foreground transition-colors">
          View →
        </Link>
      </div>
      {claims.length === 0 ? (
        <p className="type-caption text-muted-foreground">{emptyLabel}</p>
      ) : (
        <div className="space-y-2">
          {claims.map((claim) => (
            <Link
              key={claim.claimId}
              href={`/claims/${claim.claimId}`}
              className="flex items-center justify-between gap-3 border-t border-border pt-2 first:border-t-0 first:pt-0"
            >
              <span className="type-mono type-caption text-foreground truncate">
                {claim.claimId}
              </span>
              <span className="type-caption text-muted-foreground">
                {claim.analyzedAt ? formatDaysAgo(claim.analyzedAt) : "Pending"}
              </span>
            </Link>
          ))}
        </div>
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
