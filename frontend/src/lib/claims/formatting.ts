import type { ClaimTimelineEvent } from "@/lib/claims/types";

export function formatFeatureValue(value: number | null): string {
  if (value === null) return "—";
  if (Number.isInteger(value)) return String(value);
  return value.toFixed(2);
}

export function formatDateTime(value: string | null | undefined): string {
  if (!value) return "—";
  return new Date(value).toLocaleString();
}

export function formatEventLabel(event: ClaimTimelineEvent): string {
  switch (event.eventType) {
    case "analysis_generated":
      return "Analysis generated";
    case "status_changed":
      return `Status changed to ${String(event.metadata?.status ?? "updated")}`;
    case "feedback_recorded":
      return `Feedback marked ${
        event.metadata?.rating === "useful" ? "useful" : "not useful"
      }`;
    case "report_generated":
      return "Review report generated";
    default:
      return event.eventType;
  }
}
