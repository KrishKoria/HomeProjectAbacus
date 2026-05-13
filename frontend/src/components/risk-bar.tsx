interface RiskBarProps {
  score: number | null;
  level: string | null;
}

export function RiskBar({ score, level }: RiskBarProps) {
  if (score === null || level === null) {
    return (
      <div className="flex items-center gap-2">
        <span className="type-mono tabular-nums text-[12.5px] w-9 text-right text-muted-foreground opacity-50">
          —
        </span>
        <div className="flex gap-0.5 w-16">
          {[0, 1, 2, 3].map((i) => (
            <div key={i} className="h-2 flex-1 bg-border/30" />
          ))}
        </div>
      </div>
    );
  }

  const normalized = level.toLowerCase();
  const filled = Math.round(score * 4);
  const pct = Math.round(score * 100);

  return (
    <div className="flex items-center gap-2">
      <span className="type-mono tabular-nums text-[12.5px] w-9 text-right">{pct}%</span>
      <div className="flex gap-0.5 w-16">
        {[0, 1, 2, 3].map((i) => (
          <div
            key={i}
            className={`h-2 flex-1 ${i < filled ? `bg-risk-${normalized}` : "bg-border/60"}`}
          />
        ))}
      </div>
    </div>
  );
}
