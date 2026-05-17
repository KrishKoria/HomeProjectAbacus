import Link from "next/link";
import type { ClaimReview } from "@/lib/db/claims";

export function QueueBucket({
  claims,
  description,
  emptyLabel,
  href,
  title,
}: {
  claims: ClaimReview[];
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
              className="flex items-center gap-3 border-t border-border pt-2 first:border-t-0 first:pt-0"
            >
              <span className="type-mono type-caption text-foreground truncate flex-1 min-w-0">
                {claim.claimId}
              </span>
              <span className="type-caption text-muted-foreground capitalize shrink-0">
                {claim.status}
              </span>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}
