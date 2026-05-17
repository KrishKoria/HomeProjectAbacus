import { Skeleton } from "@/components/ui/skeleton";

export function ClaimsTableSkeleton() {
  return (
    <div role="status" aria-label="Loading claims">
      <span className="sr-only">Loading claims…</span>
      <div className="border border-border overflow-x-auto">
        <div className="grid min-w-135 grid-cols-[minmax(120px,1fr)_minmax(120px,2fr)_minmax(120px,1fr)_minmax(100px,1fr)_minmax(100px,1fr)] gap-4 border-b border-border px-4 py-3">
          {Array.from({ length: 5 }).map((_, index) => (
            <Skeleton key={index} className="h-3 w-full" />
          ))}
        </div>
        {Array.from({ length: 8 }).map((_, index) => (
          <div
            key={index}
            className="grid min-w-135 grid-cols-[minmax(120px,1fr)_minmax(120px,2fr)_minmax(120px,1fr)_minmax(100px,1fr)_minmax(100px,1fr)] gap-4 border-b border-border px-4 py-3 last:border-b-0"
          >
            <Skeleton className="h-4 w-full" />
            <Skeleton className="h-4 w-2/3" />
            <Skeleton className="h-4 w-full" />
            <Skeleton className="h-4 w-3/4" />
            <Skeleton className="h-4 w-1/2" />
          </div>
        ))}
      </div>
    </div>
  );
}
