"use client";

interface ClaimsFilterBarProps {
  riskFilter: string;
  statusFilter: string;
  onRiskChange: (risk: string | null) => void;
  onStatusChange: (status: string | null) => void;
}

export function ClaimsFilterBar({
  riskFilter,
  statusFilter,
  onRiskChange,
  onStatusChange,
}: ClaimsFilterBarProps) {
  return (
    <>
      <div
        aria-label="Filter by risk level"
        className="flex items-center border border-border"
        role="radiogroup"
      >
        {(["all", "high", "medium", "low"] as const).map((level, i) => (
          <button
            key={level}
            role="radio"
            aria-checked={riskFilter === level}
            className={`h-8 px-2.5 text-label font-medium transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring pointer-coarse:h-11 ${
              i > 0 ? "border-l border-border" : ""
            } ${
              riskFilter === level
                ? "bg-foreground text-background"
                : "text-muted-foreground hover:text-foreground hover:bg-muted"
            }`}
            onClick={() =>
              onRiskChange(level === "all" ? null : level)
            }
            type="button"
          >
            {level === "all"
              ? "All"
              : level.charAt(0).toUpperCase() + level.slice(1)}
          </button>
        ))}
      </div>

      <div
        aria-label="Filter by status"
        className="flex items-center border border-border"
        role="radiogroup"
      >
        {(["all", "new", "reviewed", "actioned"] as const).map(
          (status, i) => (
            <button
              key={status}
              role="radio"
              aria-checked={statusFilter === status}
              className={`h-8 px-2.5 text-label font-medium transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring pointer-coarse:h-11 ${
                i > 0 ? "border-l border-border" : ""
              } ${
                statusFilter === status
                  ? "bg-foreground text-background"
                  : "text-muted-foreground hover:text-foreground hover:bg-muted"
              }`}
              onClick={() =>
                onStatusChange(status === "all" ? null : status)
              }
              type="button"
            >
              {status === "all"
                ? "All"
                : status.charAt(0).toUpperCase() + status.slice(1)}
            </button>
          ),
        )}
      </div>
    </>
  );
}
