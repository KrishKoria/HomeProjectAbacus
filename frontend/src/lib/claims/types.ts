export interface ClaimStatusPayload {
  claimId: string;
  reviewedAt: string | null;
  reviewedByEmail: string | null;
  reviewedById: string | null;
  status: string;
}

export interface ClaimFeedbackPayload {
  feedback: {
    claimId: string;
    comment: string;
    createdAt: string;
    rating: "useful" | "not_useful";
    reason:
      | "wrong_risk_reason"
      | "missing_policy"
      | "too_vague"
      | "not_actionable"
      | null;
    userId: string;
  } | null;
}

export interface ClaimTimelineEvent {
  actorEmail: string | null;
  claimId: string;
  createdAt: string;
  eventType:
    | "analysis_generated"
    | "status_changed"
    | "feedback_recorded"
    | "report_generated";
  metadata: Record<string, unknown> | null;
}

export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}
