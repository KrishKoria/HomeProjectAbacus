import Link from "next/link";

interface WorkflowStatusProps {
  stats: {
    status: { new: number; reviewed: number; actioned: number };
  };
}

export function WorkflowStatus({ stats }: WorkflowStatusProps) {
  return (
    <div className="space-y-3">
      <p className="type-label text-muted-foreground">Workflow</p>
      <div className="space-y-2">
        {(["new", "reviewed", "actioned"] as const).map((s) => (
          <Link
            key={s}
            href={`/claims?status=${s}`}
            className="w-full flex items-center justify-between border border-border px-3 py-2.5 hover:bg-muted/50 transition-colors group"
          >
            <span className="type-caption text-muted-foreground capitalize group-hover:text-foreground transition-colors">
              {s}
            </span>
            <span className="type-mono tabular-nums text-foreground font-medium group-hover:underline underline-offset-4 decoration-foreground/30">
              {stats.status[s]}
            </span>
          </Link>
        ))}
      </div>

      <div className="border-t border-border/60 pt-3 mt-1">
        <p className="type-label text-muted-foreground">Team throughput</p>
        <p className="type-mono tabular-nums text-foreground mt-1">
          {stats.status.actioned}
        </p>
        <p className="type-caption text-muted-foreground">
          Actioned (all time)
        </p>
      </div>
    </div>
  );
}
