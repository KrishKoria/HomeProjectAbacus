const statusColors: Record<string, string> = {
  actioned: "bg-accent text-accent-foreground",
  new: "bg-muted text-muted-foreground",
  reviewed: "bg-primary/10 text-primary",
};

export function StatusBadge({ status }: { status: string }) {
  const cls = statusColors[status] ?? "bg-muted text-muted-foreground";
  const label = status.charAt(0).toUpperCase() + status.slice(1);

  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 text-xs font-medium ${cls}`}
    >
      {label}
    </span>
  );
}
