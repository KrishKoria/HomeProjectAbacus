import { cn } from "@/lib/utils";

export const riskColorMap: Record<string, string> = {
  high: "bg-risk-high-bg text-risk-high",
  medium: "bg-risk-medium-bg text-risk-medium",
  low: "bg-risk-low-bg text-risk-low",
};

export const riskProgressColorMap: Record<string, string> = {
  high: "var(--risk-high)",
  medium: "var(--risk-medium)",
  low: "var(--risk-low)",
};

interface RiskBadgeProps {
  level: string | null;
  className?: string;
}

export function RiskBadge({ level, className }: RiskBadgeProps) {
  if (!level) {
    return (
      <span className={cn("inline-flex items-center px-2 py-0.5 text-xs font-medium bg-muted text-muted-foreground opacity-50", className)}>
        &mdash;
      </span>
    );
  }
  const normalized = level.toLowerCase();
  const cls = riskColorMap[normalized] ?? "bg-muted text-muted-foreground";
  return (
    <span className={cn("inline-flex items-center px-2 py-0.5 text-xs font-medium capitalize", cls, className)}>
      {normalized}
    </span>
  );
}
