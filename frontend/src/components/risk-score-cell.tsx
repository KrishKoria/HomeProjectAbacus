function riskLevel(score: number): "low" | "medium" | "high" {
  if (score >= 0.7) return "high";
  if (score >= 0.4) return "medium";
  return "low";
}

export function RiskScoreCell({ score }: { score: number | null }) {
  if (score === null) {
    return (
      <div className="flex items-center gap-2">
        <span className="type-mono tabular-nums w-8 text-right text-muted-foreground opacity-50">
          —
        </span>
        <div className="w-16 h-1.5 bg-muted overflow-hidden opacity-30" />
      </div>
    );
  }

  const level = riskLevel(score);
  const pct = Math.round(score * 100);

  return (
    <div className="flex items-center gap-2">
      <div className="w-16 h-1.5 bg-muted overflow-hidden">
        <div
          className="h-full transition-all duration-700 ease-out"
          style={{
            width: `${pct}%`,
            backgroundColor: `var(--risk-${level})`,
          }}
        />
      </div>
      <span className="type-mono tabular-nums">{pct}%</span>
    </div>
  );
}
