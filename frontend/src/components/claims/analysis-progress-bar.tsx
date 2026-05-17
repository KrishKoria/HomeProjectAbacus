"use client";

interface AnalysisProgress {
  completed: number;
  current: string | null;
  failed: number;
  total: number;
}

export function AnalysisProgressBar({
  isProcessing,
  progress,
}: {
  isProcessing: boolean;
  progress: AnalysisProgress;
}) {
  if (progress.total === 0) return null;

  return (
    <div className="space-y-2 border border-border px-4 py-3">
      <div className="flex items-center gap-3">
        <div
          aria-label="Analysis progress"
          aria-valuemax={100}
          aria-valuemin={0}
          aria-valuenow={
            progress.total === 0
              ? 0
              : Math.round(
                  ((progress.completed + progress.failed) /
                    progress.total) *
                    100,
                )
          }
          className="h-0.5 flex-1 overflow-hidden bg-muted"
          role="progressbar"
        >
          <div
            className="h-full bg-primary transition-all duration-500"
            style={{
              width: `${progress.total === 0 ? 0 : Math.round(((progress.completed + progress.failed) / progress.total) * 100)}%`,
            }}
          />
        </div>
        <span className="type-caption shrink-0 tabular-nums text-muted-foreground">
          {isProcessing ? "Analyzing" : "Queued"}{" "}
          {progress.completed + progress.failed} / {progress.total}
        </span>
      </div>
      {(progress.failed > 0 || progress.current) && (
        <div className="flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
          {progress.current && <span>Current: {progress.current}</span>}
          {progress.failed > 0 && <span>{progress.failed} failed</span>}
        </div>
      )}
    </div>
  );
}
