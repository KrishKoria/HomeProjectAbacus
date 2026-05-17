import Link from "next/link";

interface RiskDistributionProps {
  stats: {
    risk: { high: number; medium: number; low: number };
    total: number;
  };
}

export function RiskDistribution({ stats }: RiskDistributionProps) {
  return (
    <div className="space-y-3">
      <p className="type-label text-muted-foreground">
        Risk distribution
      </p>
      <div className="space-y-2.5">
        {(["high", "medium", "low"] as const).map((level) => {
          const count = stats.risk[level];
          const pct =
            stats.total > 0 ? (count / stats.total) * 100 : 0;
          return (
            <Link
              key={level}
              href={`/claims?risk=${level}`}
              className="w-full grid grid-cols-[56px_1fr_28px] items-center gap-3 text-label group"
            >
              <span
                className={`text-right type-mono text-risk-${level} capitalize group-hover:opacity-80 transition-opacity`}
              >
                {level}
              </span>
              <div className="relative h-2 bg-muted overflow-hidden">
                <div
                  className={`absolute inset-y-0 left-0 bg-risk-${level}`}
                  style={{ width: `${pct}%` }}
                />
              </div>
              <span className="type-mono tabular-nums text-foreground text-right">
                {count}
              </span>
            </Link>
          );
        })}
      </div>
    </div>
  );
}
