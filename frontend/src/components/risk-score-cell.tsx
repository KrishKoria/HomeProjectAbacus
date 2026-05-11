function riskLevel(score: number): "low" | "medium" | "high" {
  if (score >= 0.7) return "high";
  if (score >= 0.4) return "medium";
  return "low";
}

export function RiskScoreCell({ score }: { score: number }) {
  const level = riskLevel(score);
  const pct = (score * 100).toFixed(0);

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
