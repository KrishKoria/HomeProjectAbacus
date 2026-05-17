"use client";

import { useState } from "react";
import { ThumbsDown, ThumbsUp } from "@phosphor-icons/react";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { ClaimFeedbackPayload } from "@/lib/claims/types";

export function FeedbackSection({
  initialFeedback,
  isPending,
  onSubmit,
}: {
  initialFeedback: ClaimFeedbackPayload["feedback"];
  isPending: boolean;
  onSubmit: (payload: {
    comment: string;
    rating: "useful" | "not_useful";
    reason:
      | "wrong_risk_reason"
      | "missing_policy"
      | "too_vague"
      | "not_actionable"
      | null;
  }) => void;
}) {
  const [comment, setComment] = useState(initialFeedback?.comment ?? "");
  const [reason, setReason] = useState<
    "wrong_risk_reason" | "missing_policy" | "too_vague" | "not_actionable" | ""
  >(
    (initialFeedback?.reason ?? "") as
      | "wrong_risk_reason"
      | "missing_policy"
      | "too_vague"
      | "not_actionable"
      | "",
  );
  const [rating, setRating] = useState<"useful" | "not_useful" | null>(
    initialFeedback?.rating ?? null,
  );

  return (
    <div className="px-5 py-4 space-y-4">
      <div className="flex flex-wrap gap-2">
        <Button
          type="button"
          variant={rating === "useful" ? "default" : "outline"}
          size="sm"
          disabled={isPending}
          onClick={() => {
            onSubmit({
              comment,
              rating: "useful",
              reason: reason || null,
            });
            setRating("useful");
          }}
        >
          <ThumbsUp data-icon="inline-start" />
          Useful
        </Button>
        <Button
          type="button"
          variant={rating === "not_useful" ? "default" : "outline"}
          size="sm"
          disabled={isPending}
          onClick={() => {
            onSubmit({
              comment,
              rating: "not_useful",
              reason: reason || null,
            });
            setRating("not_useful");
          }}
        >
          <ThumbsDown data-icon="inline-start" />
          Not useful
        </Button>
      </div>

      <div className="space-y-2">
        <p className="type-label text-muted-foreground">Reason</p>
        <Select
          items={[
            { value: "wrong_risk_reason", label: "Wrong risk reason" },
            { value: "missing_policy", label: "Missing policy" },
            { value: "too_vague", label: "Too vague" },
            { value: "not_actionable", label: "Not actionable" },
          ]}
          value={reason}
          onValueChange={(value) => setReason((value as typeof reason) ?? "")}
        >
          <SelectTrigger
            className="w-full max-w-sm"
            aria-label="Feedback reason"
          >
            <SelectValue placeholder="Optional reason" />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              <SelectItem value="wrong_risk_reason">
                Wrong risk reason
              </SelectItem>
              <SelectItem value="missing_policy">Missing policy</SelectItem>
              <SelectItem value="too_vague">Too vague</SelectItem>
              <SelectItem value="not_actionable">Not actionable</SelectItem>
            </SelectGroup>
          </SelectContent>
        </Select>
      </div>

      <div className="space-y-2">
        <p className="type-label text-muted-foreground">Comment</p>
        <textarea
          value={comment}
          onChange={(event) => setComment(event.target.value)}
          rows={3}
          className="w-full max-w-xl resize-none bg-transparent border border-border px-3 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
          placeholder="Optional feedback for future model improvements"
        />
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={isPending}
          onClick={() =>
            onSubmit({
              comment,
              rating: rating ?? "useful",
              reason: reason || null,
            })
          }
        >
          Save feedback detail
        </Button>
      </div>
    </div>
  );
}
