"use client";

import { Question } from "@phosphor-icons/react";
import { Button } from "@/components/ui/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";

const riskRows: { color: string; label: string; threshold: string }[] = [
  { color: "bg-risk-high", label: "High", threshold: "≥ 70%" },
  { color: "bg-risk-medium", label: "Medium", threshold: "40–69%" },
  { color: "bg-risk-low", label: "Low", threshold: "< 40%" },
];

const workflowRows: { label: string; definition: string }[] = [
  { label: "New", definition: "Claim arrived, not yet reviewed by an analyst." },
  {
    label: "Reviewed",
    definition: "Analyst has read the analysis and acknowledged findings.",
  },
  {
    label: "Actioned",
    definition: "Claim has been remediated or escalated to payer.",
  },
];

const shortcutRows: { keys: string[]; caption: string }[] = [
  { keys: ["/"], caption: "Focus search field" },
  { keys: ["C"], caption: "Focus chat input (detail page)" },
  { keys: ["G", "C"], caption: "Go to Claims queue" },
  { keys: ["G", "D"], caption: "Go to Dashboard" },
  { keys: ["j", "k"], caption: "Move between table rows" },
  { keys: ["Enter"], caption: "Open focused claim" },
  { keys: ["n"], caption: "Next claim (detail page)" },
];

export function QueueHelpPanel() {
  return (
    <Popover>
      <PopoverTrigger
        render={
          <Button
            aria-label="Queue help — risk thresholds, workflow states, keyboard shortcuts"
            size="icon-sm"
            variant="ghost"
          />
        }
      >
        <Question className="size-4" aria-hidden="true" />
      </PopoverTrigger>

      <PopoverContent
        align="end"
        className="w-80 p-0"
        side="bottom"
        sideOffset={6}
      >
        <div className="divide-y divide-border">
          {/* Risk thresholds */}
          <section className="px-4 py-3">
            <p className="type-label mb-2 text-muted-foreground uppercase tracking-wider">
              Risk thresholds
            </p>
            <ul className="space-y-1.5">
              {riskRows.map(({ color, label, threshold }) => (
                <li key={label} className="flex items-center gap-2">
                  <span
                    className={`size-2 shrink-0 rounded-none ${color}`}
                    aria-hidden="true"
                  />
                  <span className="type-body flex-1">{label}</span>
                  <span className="type-caption text-muted-foreground tabular-nums">
                    {threshold}
                  </span>
                </li>
              ))}
            </ul>
          </section>

          {/* Workflow states */}
          <section className="px-4 py-3">
            <p className="type-label mb-2 text-muted-foreground uppercase tracking-wider">
              Workflow states
            </p>
            <ul className="space-y-1.5">
              {workflowRows.map(({ label, definition }) => (
                <li key={label} className="flex gap-2">
                  <span className="type-label w-16 shrink-0 pt-px">{label}</span>
                  <span className="type-caption text-muted-foreground leading-snug">
                    {definition}
                  </span>
                </li>
              ))}
            </ul>
          </section>

          {/* Keyboard shortcuts */}
          <section className="px-4 py-3">
            <p className="type-label mb-2 text-muted-foreground uppercase tracking-wider">
              Keyboard shortcuts
            </p>
            <ul className="space-y-1.5">
              {shortcutRows.map(({ keys, caption }) => (
                <li
                  key={keys.join("+")}
                  className="flex items-center gap-2"
                >
                  <span className="flex shrink-0 items-center gap-0.5">
                    {keys.map((k, i) => (
                      <span key={i} className="flex items-center gap-0.5">
                        <kbd className="inline-flex h-5 min-w-5 items-center justify-center border border-border bg-muted px-1 font-mono type-caption text-foreground">
                          {k}
                        </kbd>
                        {i < keys.length - 1 && (
                          <span className="type-caption text-muted-foreground">
                            then
                          </span>
                        )}
                      </span>
                    ))}
                  </span>
                  <span className="type-caption text-muted-foreground">
                    {caption}
                  </span>
                </li>
              ))}
            </ul>
          </section>
        </div>
      </PopoverContent>
    </Popover>
  );
}
