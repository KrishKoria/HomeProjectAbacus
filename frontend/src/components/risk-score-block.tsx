"use client";

import { RiskBadge } from "@/components/risk-badge";
import { useCounter } from "@/hooks/use-counter";

interface RiskScoreBlockProps {
  score: number;
  level: string;
  analyzedAt?: string | null;
}

const subtext: Record<string, string> = {
  high: "Strong denial signal. Resolve findings before submission.",
  medium: "Mixed signals. Review findings and patch before submission.",
  low: "Clean claim. Ready to submit unless something else changes.",
};

export function RiskScoreBlock({ score, level, analyzedAt }: RiskScoreBlockProps) {
  const target = Math.round(score * 100);
  const display = useCounter(target);
  const normalized = level.toLowerCase();
  const filled = Math.round((display / 100) * 20);

  const timeStr = analyzedAt
    ? new Date(analyzedAt).toLocaleTimeString("en-US", {
        hour: "numeric",
        minute: "2-digit",
      })
    : null;

  return (
    <section className="border-b border-border pb-8">
      <div className="type-label text-muted-foreground mb-3">Denial risk</div>
      <div className="flex items-end gap-6 flex-wrap">
        <div>
          <div className="flex items-baseline gap-3">
            <span
              className="type-display tabular-nums leading-none"
              style={{ color: `var(--risk-${normalized})` }}
            >
              {display}
              <span className="text-[1.5rem] align-top ml-0.5 opacity-70">%</span>
            </span>
            <RiskBadge level={normalized} />
          </div>
          <p className="text-[12.5px] text-muted-foreground mt-2 max-w-[42ch]">
            {subtext[normalized] ?? subtext.low}
          </p>
        </div>

        {/* 20-segment bar — clinical instrument feel */}
        <div className="flex-1 min-w-[280px] max-w-[420px]">
          <div className="flex gap-[2px] items-end" style={{ height: 24 }}>
            {Array.from({ length: 20 }).map((_, i) => {
              const on = i < filled;
              const h = 8 + (i / 19) * 16;
              return (
                <div key={i} className="flex-1 flex items-end" style={{ height: 24 }}>
                  <div
                    className="w-full"
                    style={{
                      height: h,
                      backgroundColor: on
                        ? `var(--risk-${normalized})`
                        : "oklch(0.88 0.015 260 / 0.6)",
                      transition: "background-color 220ms ease-out",
                    }}
                  />
                </div>
              );
            })}
          </div>
          <div className="flex justify-between text-[10px] text-muted-foreground type-mono mt-1.5">
            <span>0%</span>
            <span>50%</span>
            <span>100%</span>
          </div>
        </div>

        {timeStr && (
          <div className="text-[11px] text-muted-foreground type-mono tabular-nums text-right shrink-0">
            <div>
              <span className="text-foreground">{timeStr}</span> · today
            </div>
          </div>
        )}
      </div>
    </section>
  );
}
