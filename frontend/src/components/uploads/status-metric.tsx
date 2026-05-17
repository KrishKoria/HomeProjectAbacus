export function StatusMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <p className="type-label text-muted-foreground">{label}</p>
      <p className="truncate font-mono text-xs text-foreground">{value}</p>
    </div>
  );
}
