"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { use, useEffect, useRef, useState } from "react";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { AppShell } from "@/components/app-shell";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { RiskScoreBlock } from "@/components/risk-score-block";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { toast } from "sonner";
import { buildRemediationChecklist } from "@/lib/claims/remediation-checklist";
import type { ClaimAnalysisResponse } from "@/lib/databricks/types";
import {
  ArrowLeft,
  Copy,
  DownloadSimple,
  Files,
  Warning,
  PaperPlaneTilt,
  ChatText,
  ThumbsDown,
  ThumbsUp,
} from "@phosphor-icons/react";

// ─── Risk / Direction helpers ───────────────────────────────────────────────

function DirectionTag({ direction }: { direction: string }) {
  if (direction === "increases_risk")
    return (
      <Tooltip>
        <TooltipTrigger
          render={<span />}
          className="inline-flex items-center px-2 py-0.5 text-xs font-medium bg-direction-up-bg text-direction-up cursor-default"
        >
          Raises denial risk
        </TooltipTrigger>
        <TooltipContent side="top">
          This feature pushed the model toward predicting denial. Higher
          contribution = stronger signal.
        </TooltipContent>
      </Tooltip>
    );
  if (direction === "decreases_risk")
    return (
      <Tooltip>
        <TooltipTrigger
          render={<span />}
          className="inline-flex items-center px-2 py-0.5 text-xs font-medium bg-direction-down-bg text-direction-down cursor-default"
        >
          Lowers denial risk
        </TooltipTrigger>
        <TooltipContent side="top">
          This feature pulled the model away from predicting denial.
        </TooltipContent>
      </Tooltip>
    );
  return (
    <span className="inline-flex items-center px-2 py-0.5 text-xs font-medium bg-muted text-muted-foreground">
      Neutral
    </span>
  );
}

function formatFeatureValue(value: number | null): string {
  if (value === null) return "—";
  if (Number.isInteger(value)) return String(value);
  return value.toFixed(2);
}

function formatDateTime(value: string | null | undefined): string {
  if (!value) return "—";
  return new Date(value).toLocaleString();
}

interface ClaimStatusPayload {
  claimId: string;
  reviewedAt: string | null;
  reviewedByEmail: string | null;
  reviewedById: string | null;
  status: string;
}

interface ClaimFeedbackPayload {
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

interface ClaimTimelineEvent {
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

function formatEventLabel(event: ClaimTimelineEvent): string {
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

// ─── Chat panel ──────────────────────────────────────────────────────────────

interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

function ChatPanel({
  claimId,
  analysis,
  queuedQuestion,
  onQueuedQuestionHandled,
}: {
  claimId: string;
  analysis: ClaimAnalysisResponse | undefined;
  queuedQuestion: string | null;
  onQueuedQuestionHandled: () => void;
}) {
  const chatInputRef = useRef<HTMLTextAreaElement>(null);
  const threadRef = useRef<HTMLDivElement>(null);
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>(() => []);
  const [isWaiting, setIsWaiting] = useState(false);
  const openingMessage =
    analysis && messages.length === 0
      ? {
          role: "assistant" as const,
          content: `This claim scored ${Math.round(analysis.riskScore * 100)}%: ${
            analysis.riskLevel.charAt(0).toUpperCase() +
            analysis.riskLevel.slice(1)
          } denial risk. Ask me anything about it.`,
        }
      : null;
  const threadMessageCount = messages.length + (openingMessage ? 1 : 0);

  useEffect(() => {
    if (threadRef.current) {
      threadRef.current.scrollTop = threadRef.current.scrollHeight;
    }
  }, [threadMessageCount, isWaiting]);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (
        e.key === "c" &&
        !e.metaKey &&
        !e.ctrlKey &&
        document.activeElement?.tagName !== "INPUT" &&
        document.activeElement?.tagName !== "TEXTAREA"
      ) {
        e.preventDefault();
        chatInputRef.current?.focus();
      }
      if (
        e.key === "/" &&
        document.activeElement?.tagName !== "INPUT" &&
        document.activeElement?.tagName !== "TEXTAREA"
      ) {
        e.preventDefault();
        chatInputRef.current?.focus();
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  async function sendMessage(overrideText?: string) {
    const text = (overrideText ?? input).trim();
    if (!text || isWaiting || !analysis) return;
    if (!overrideText) setInput("");
    const userMsg: ChatMessage = { role: "user", content: text };
    const nextMessages = [...messages, userMsg];
    setMessages(nextMessages);
    setIsWaiting(true);

    try {
      const res = await fetch(`/api/claims/${claimId}/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          messages: nextMessages,
          claimContext: {
            claimId: analysis.claimId,
            riskScore: analysis.riskScore,
            riskLevel: analysis.riskLevel,
            narrative: analysis.narrative,
            topReasons: analysis.topReasons.map((r) => ({
              description: r.description,
              direction: r.direction,
            })),
          },
        }),
      });
      const data = await res.json();
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: data.reply ?? "Sorry, I couldn't get a response.",
        },
      ]);
    } catch {
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: "Chat unavailable. Please try again." },
      ]);
    } finally {
      setIsWaiting(false);
    }
  }

  const sendMessageRef = useRef(sendMessage);

  useEffect(() => {
    sendMessageRef.current = sendMessage;
  });

  useEffect(() => {
    if (!queuedQuestion || !analysis || isWaiting) return;
    void sendMessageRef.current(queuedQuestion);
    onQueuedQuestionHandled();
  }, [queuedQuestion, analysis, isWaiting, onQueuedQuestionHandled]);

  function onKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  }

  // Build Q+A pairs from messages array
  const qaPairs: Array<{ question: string; answer: string | null }> = [];
  for (let i = 0; i < messages.length; i++) {
    if (messages[i].role === "user") {
      const next = messages[i + 1];
      qaPairs.push({
        question: messages[i].content,
        answer: next?.role === "assistant" ? next.content : null,
      });
      if (next?.role === "assistant") i++; // skip the answer in outer loop
    }
  }

  return (
    <div className="flex flex-col h-full">
      <div className="px-5 py-3 border-b border-border shrink-0">
        <div className="flex items-center gap-2">
          <ChatText
            className="size-4 text-muted-foreground"
            aria-hidden="true"
          />
          <span className="type-title">Ask about this claim</span>
        </div>
        <p className="type-caption text-muted-foreground mt-0.5">
          Press{" "}
          <kbd className="font-mono type-caption px-1 border border-border">
            C
          </kbd>{" "}
          to focus
        </p>
      </div>

      <div
        ref={threadRef}
        className="flex-1 overflow-y-auto px-4 py-4 space-y-6"
        aria-live="polite"
        aria-label="Chat messages"
      >
        {!analysis && (
          <p className="type-caption text-muted-foreground text-center mt-8">
            Waiting for analysis…
          </p>
        )}

        {/* System opener — rendered as a header above Q&A log */}
        {openingMessage && (
          <div className="pb-4 border-b border-border/40">
            <p className="type-caption text-muted-foreground uppercase tracking-wide mb-1">
              Context
            </p>
            <p className="type-body max-w-none text-muted-foreground leading-relaxed">
              {openingMessage.content}
            </p>
          </div>
        )}

        {/* Q+A log */}
        {qaPairs.map((pair, i) => (
          <div
            key={i}
            className={
              i < qaPairs.length - 1 || isWaiting
                ? "pb-6 border-b border-border/40"
                : "pb-2"
            }
          >
            {/* Question row */}
            <div className="flex items-start gap-2.5 mb-3">
              <div
                className="w-0.5 self-stretch bg-foreground/20 shrink-0 mt-0.5"
                aria-hidden="true"
              />
              <p className="type-label text-foreground leading-snug">
                {pair.question}
              </p>
            </div>
            {/* Answer row */}
            {pair.answer !== null && (
              <div className="pl-3.25">
                <p className="type-caption text-muted-foreground mb-1">
                  Answer
                </p>
                <p className="type-body max-w-none text-foreground leading-relaxed">
                  {pair.answer}
                </p>
              </div>
            )}
          </div>
        ))}

        {/* Suggested questions — shown when no user messages yet */}
        {messages.length === 0 &&
          analysis &&
          (() => {
            const suggestionMap: Record<string, string[]> = {
              high: [
                "What's the single fastest fix?",
                "Show me the policy rule that's failing",
                "Draft the remediation note",
              ],
              medium: [
                "What changes if I tighten the diagnosis code?",
                "Is the documentation enough as-is?",
                "Should I escalate for medical review?",
              ],
              low: [
                "Anything I should still check?",
                "Why is this still in the queue?",
                "Confidence level on the score?",
              ],
            };
            const items = suggestionMap[analysis.riskLevel.toLowerCase()] ?? [];
            if (!items.length) return null;
            return (
              <div className="mt-2">
                <p className="type-label text-muted-foreground mb-3">
                  Suggested
                </p>
                <div className="space-y-1.5">
                  {items.map((s) => (
                    <button
                      key={s}
                      type="button"
                      onClick={() => sendMessage(s)}
                      className="block w-full text-left type-label text-foreground/80 hover:text-foreground border border-border hover:border-foreground/30 px-2.5 py-1.5 transition-colors"
                    >
                      {s}
                    </button>
                  ))}
                </div>
              </div>
            );
          })()}

        {/* Typing indicator */}
        {isWaiting && (
          <div className="pl-3.25">
            <p className="type-caption text-muted-foreground mb-1">Answer</p>
            <div className="flex items-center gap-1 py-1">
              <span className="size-1.5 bg-muted-foreground rounded-full animate-pulse" />
              <span
                className="size-1.5 bg-muted-foreground rounded-full animate-pulse"
                style={{ animationDelay: "150ms" }}
              />
              <span
                className="size-1.5 bg-muted-foreground rounded-full animate-pulse"
                style={{ animationDelay: "300ms" }}
              />
            </div>
          </div>
        )}
      </div>

      <div className="px-4 py-3 border-t border-border shrink-0">
        <div className="flex items-end gap-2">
          <textarea
            ref={chatInputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={onKeyDown}
            placeholder="Ask about this claim…"
            rows={2}
            disabled={!analysis || isWaiting}
            aria-label="Ask a question about this claim"
            className="flex-1 resize-none bg-transparent border border-border px-3 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring disabled:opacity-50 min-h-10 max-h-28"
          />
          <Button
            size="icon"
            onClick={() => sendMessage()}
            disabled={!input.trim() || isWaiting || !analysis}
            className="shrink-0"
            aria-label="Send message"
          >
            <PaperPlaneTilt aria-hidden="true" />
          </Button>
        </div>
      </div>
    </div>
  );
}

// ─── Status control ───────────────────────────────────────────────────────────

function StatusControl({
  claimId,
  initialStatus,
}: {
  claimId: string;
  initialStatus: string;
}) {
  const queryClient = useQueryClient();
  const [status, setStatus] = useState(initialStatus);
  const previousStatusRef = useRef(initialStatus);

  const mutation = useMutation({
    mutationFn: async (newStatus: string) => {
      const res = await fetch(`/api/claims/${claimId}/status`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: newStatus }),
      });
      if (!res.ok) throw new Error("Failed to update status");
      return res.json();
    },
    onMutate: async () => {
      previousStatusRef.current = status;
    },
    onSuccess: (_, newStatus) => {
      setStatus(newStatus);
      const label = newStatus.charAt(0).toUpperCase() + newStatus.slice(1);
      const undoStatus = previousStatusRef.current;
      toast(`Marked as ${label}`, {
        action: {
          label: "Undo",
          onClick: () => mutation.mutate(undoStatus),
        },
        duration: 5000,
      });
      queryClient.invalidateQueries({ queryKey: ["claims"] });
      queryClient.invalidateQueries({ queryKey: ["claim-status", claimId] });
    },
    onError: () => toast.error("Could not update status"),
  });

  const items = [
    { value: "new", label: "New" },
    { value: "reviewed", label: "Reviewed" },
    { value: "actioned", label: "Actioned" },
  ] as const;

  return (
    <Select
      items={items}
      value={status}
      onValueChange={(v) => {
        if (v && v !== status) mutation.mutate(v);
      }}
      disabled={mutation.isPending}
    >
      <SelectTrigger
        className="w-36 h-10 pointer-coarse:h-11 text-xs"
        aria-label="Claim status"
      >
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {items.map((item) => (
            <SelectItem key={item.value} value={item.value}>
              {item.label}
            </SelectItem>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  );
}

function FeedbackSection({
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

  return (
    <div className="px-5 py-4 space-y-4">
      <div className="flex flex-wrap gap-2">
        <Button
          type="button"
          variant={initialFeedback?.rating === "useful" ? "default" : "outline"}
          size="sm"
          disabled={isPending}
          onClick={() =>
            onSubmit({
              comment,
              rating: "useful",
              reason: reason || null,
            })
          }
        >
          <ThumbsUp data-icon="inline-start" />
          Useful
        </Button>
        <Button
          type="button"
          variant={
            initialFeedback?.rating === "not_useful" ? "default" : "outline"
          }
          size="sm"
          disabled={isPending}
          onClick={() =>
            onSubmit({
              comment,
              rating: "not_useful",
              reason: reason || null,
            })
          }
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
              rating: initialFeedback?.rating ?? "useful",
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

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function ClaimDetailPage({
  params,
}: {
  params: Promise<{ claimId: string }>;
}) {
  const { claimId } = use(params);
  const queryClient = useQueryClient();
  const headingRef = useRef<HTMLHeadingElement>(null);
  const [chatOpen, setChatOpen] = useState(false);
  const [queuedChatQuestion, setQueuedChatQuestion] = useState<string | null>(
    null,
  );
  const [retryCount, setRetryCount] = useState(0);
  const router = useRouter();

  const analysisQuery = useQuery({
    queryKey: ["claim-analysis", claimId],
    queryFn: async () => {
      const res = await fetch("/api/claims/analyze", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ claimId }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: "Unknown error" }));
        throw new Error(err.error ?? "Analysis failed");
      }
      return res.json() as Promise<ClaimAnalysisResponse>;
    },
    retry: false,
  });

  const claimStatusQuery = useQuery({
    queryKey: ["claim-status", claimId],
    queryFn: async () => {
      const res = await fetch(
        `/api/claims/${encodeURIComponent(claimId)}/status`,
      );
      if (!res.ok) {
        return {
          claimId,
          reviewedAt: null,
          reviewedByEmail: null,
          reviewedById: null,
          status: "new",
        } satisfies ClaimStatusPayload;
      }
      return (await res.json()) as ClaimStatusPayload;
    },
    staleTime: 5 * 60 * 1000,
  });

  const feedbackQuery = useQuery({
    queryKey: ["claim-feedback", claimId],
    queryFn: async () => {
      const res = await fetch(
        `/api/claims/${encodeURIComponent(claimId)}/feedback`,
      );
      if (!res.ok) throw new Error("Failed to load claim feedback");
      return (await res.json()) as ClaimFeedbackPayload;
    },
    enabled: !!analysisQuery.data,
  });

  const timelineQuery = useQuery({
    queryKey: ["claim-timeline", claimId],
    queryFn: async () => {
      const res = await fetch(
        `/api/claims/${encodeURIComponent(claimId)}/timeline`,
      );
      if (!res.ok) throw new Error("Failed to load claim activity");
      return (await res.json()) as { events: ClaimTimelineEvent[] };
    },
    enabled: !!analysisQuery.data,
  });

  const feedbackMutation = useMutation({
    mutationFn: async (payload: {
      comment: string;
      rating: "useful" | "not_useful";
      reason:
        | "wrong_risk_reason"
        | "missing_policy"
        | "too_vague"
        | "not_actionable"
        | null;
    }) => {
      const response = await fetch(
        `/api/claims/${encodeURIComponent(claimId)}/feedback`,
        {
          body: JSON.stringify(payload),
          headers: { "Content-Type": "application/json" },
          method: "POST",
        },
      );
      if (!response.ok) {
        const errorPayload = (await response
          .json()
          .catch(() => ({ error: "Feedback failed" }))) as { error?: string };
        throw new Error(errorPayload.error ?? "Feedback failed");
      }
      return response.json() as Promise<ClaimFeedbackPayload>;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["claim-feedback", claimId] });
      queryClient.invalidateQueries({ queryKey: ["claim-timeline", claimId] });
      toast.success("Feedback saved");
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Feedback failed");
    },
  });

  // FIX 2: Next high-risk claim query
  // Uses /api/claims with risk=high&status=new&sort=riskScore&order=desc
  // to find the next high-risk unactioned claim after the current one
  const nextHighRiskQuery = useQuery({
    queryKey: ["next-high-risk", claimId],
    queryFn: async () => {
      const url = new URL("/api/claims", window.location.origin);
      url.searchParams.set("risk", "high");
      url.searchParams.set("sort", "riskScore");
      url.searchParams.set("order", "desc");
      url.searchParams.set("limit", "20");
      // Fetch new + reviewed (not actioned)
      const [resNew, resReviewed] = await Promise.all([
        fetch(url.toString() + "&status=new"),
        fetch(url.toString() + "&status=reviewed"),
      ]);
      const [dataNew, dataReviewed] = await Promise.all([
        resNew.ok ? resNew.json() : { claims: [] },
        resReviewed.ok ? resReviewed.json() : { claims: [] },
      ]);
      const allClaims: Array<{ claimId: string; riskScore: number | null }> = [
        ...(dataNew.claims ?? []),
        ...(dataReviewed.claims ?? []),
      ];
      // Sort by riskScore desc, deduplicate, find first claim that isn't current
      const seen = new Set<string>();
      const sorted = allClaims
        .filter((c) => {
          if (seen.has(c.claimId)) return false;
          seen.add(c.claimId);
          return true;
        })
        .sort((a, b) => (b.riskScore ?? 0) - (a.riskScore ?? 0));
      const next = sorted.find((c) => c.claimId !== claimId);
      return next?.claimId ?? null;
    },
    staleTime: 2 * 60 * 1000,
  });

  // FIX 3: Runtime status query (for diagnostic error messages)
  const runtimeStatusQuery = useQuery({
    queryKey: ["runtime-status"],
    queryFn: async () => {
      const res = await fetch("/api/runtime/status");
      if (!res.ok) return null;
      return res.json() as Promise<{
        analysisEndpoint?: boolean;
        sqlWarehouse?: boolean;
      }>;
    },
    enabled: analysisQuery.isError,
    staleTime: 30 * 1000,
  });

  useEffect(() => {
    if (runtimeStatusQuery.data?.sqlWarehouse === false) {
      fetch("/api/runtime/start-warehouse", { method: "POST" }).catch(() => {});
    }
  }, [runtimeStatusQuery.data]);



  useEffect(() => {
    if (analysisQuery.data) {
      queryClient.invalidateQueries({ queryKey: ["claims"] });
      headingRef.current?.focus();
    }
  }, [analysisQuery.data, queryClient]);

  // FIX 2: "n" keyboard shortcut for Next high-risk
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (
        e.key === "n" &&
        !e.metaKey &&
        !e.ctrlKey &&
        document.activeElement?.tagName !== "INPUT" &&
        document.activeElement?.tagName !== "TEXTAREA"
      ) {
        const nextId = nextHighRiskQuery.data;
        if (nextId) {
          e.preventDefault();
          router.push(`/claims/${nextId}`);
        }
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [nextHighRiskQuery.data, router]);

  const analysis = analysisQuery.data;
  const riskLevel = analysis?.riskLevel ?? "low";
  const claimStatus = claimStatusQuery.data;
  const remediationChecklist = analysis
    ? buildRemediationChecklist(analysis)
    : [];

  const breadcrumb = (
    <>
      <Link href="/claims" className="hover:text-foreground transition-colors">
        Claims
      </Link>
      <span className="mx-1.5 opacity-40">/</span>
      <span className="type-mono">{claimId}</span>
    </>
  );

  return (
    <AppShell breadcrumb={breadcrumb}>
      <div className="flex min-h-[calc(100dvh-3rem)]">
        {/* LEFT COLUMN — scrollable analysis */}
        <div className="flex-1 overflow-y-auto min-w-0">
          <div className="p-6 space-y-6 max-w-3xl">
            {/* Page header */}
            <div className="flex items-center gap-3">
              <Button
                variant="ghost"
                size="icon"
                render={<Link href="/claims" />}
                nativeButton={false}
                className="shrink-0"
                aria-label="Back to claims"
              >
                <ArrowLeft aria-hidden="true" />
              </Button>
              <h1
                ref={headingRef}
                tabIndex={-1}
                className="type-headline outline-none flex-1 min-w-0 truncate"
              >
                Claim <span className="type-mono">{claimId}</span>
              </h1>
              {analysis && (
                <StatusControl
                  key={claimStatus?.status ?? "new"}
                  claimId={claimId}
                  initialStatus={claimStatus?.status ?? "new"}
                />
              )}
            </div>

            {/* Loading */}
            {analysisQuery.isLoading && (
              <div
                role="status"
                aria-label="Loading claim analysis"
                className="space-y-4"
              >
                <span className="sr-only">Loading claim analysis…</span>
                <div className="border border-border p-5 space-y-3">
                  <Skeleton className="h-12 w-32" />
                  <Skeleton className="h-2 w-48" />
                </div>
                <div className="border border-border">
                  <div className="px-5 py-3 border-b border-border">
                    <Skeleton className="h-4 w-28" />
                  </div>
                  {Array.from({ length: 4 }).map((_, i) => (
                    <div
                      key={i}
                      className="px-5 py-3 border-b border-border last:border-b-0"
                    >
                      <Skeleton className="h-4 w-full" />
                    </div>
                  ))}
                </div>
                <div className="border border-border p-5">
                  <Skeleton className="h-4 w-full mb-2" />
                  <Skeleton className="h-4 w-4/5" />
                </div>
              </div>
            )}

            {/* FIX 3: Diagnostic error block */}
            {analysisQuery.isError &&
              (() => {
                const errMsg =
                  analysisQuery.error instanceof Error
                    ? analysisQuery.error.message
                    : "Analysis failed";
                const runtimeData = runtimeStatusQuery.data;
                let runtimeHint: string | null = null;
                if (runtimeData) {
                  if (runtimeData.analysisEndpoint === false) {
                    runtimeHint = "The model endpoint appears offline.";
                  } else if (runtimeData.sqlWarehouse === false) {
                    runtimeHint = "The SQL warehouse appears offline.";
                  }
                }
                return (
                  <section className="border border-border py-5 px-5">
                    <div className="flex items-start gap-3">
                      <Warning
                        className="size-4 text-status-err shrink-0 mt-0.5"
                        aria-hidden="true"
                      />
                      <div className="flex-1 min-w-0">
                        <p className="type-body text-foreground">{errMsg}</p>
                        {runtimeHint && (
                          <p className="type-caption text-muted-foreground mt-1">
                            {runtimeHint}
                          </p>
                        )}
                        {retryCount >= 1 && (
                          <p className="type-caption text-muted-foreground mt-1">
                            {retryCount === 1
                              ? "If the SQL warehouse was just started, wait 60 seconds then retry."
                              : "If the issue persists, contact your system administrator."}
                          </p>
                        )}
                      </div>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => {
                          setRetryCount((c) => c + 1);
                          analysisQuery.refetch();
                        }}
                        className="shrink-0"
                      >
                        Retry
                      </Button>
                    </div>
                  </section>
                );
              })()}

            {/* Analysis */}
            {analysis && (
              <>
                {/* Risk score */}
                <RiskScoreBlock
                  score={analysis.riskScore}
                  level={riskLevel}
                  analyzedAt={analysis.generatedAt}
                />

                {/* Key Findings */}
                {analysis.topReasons.length > 0 && (
                  <section className="border border-border">
                    <div className="px-5 py-3 border-b border-border">
                      <h2 className="type-title">Key Findings</h2>
                    </div>
                    <div className="divide-y divide-border">
                      {analysis.topReasons.map((reason, i) => (
                        <div
                          key={i}
                          className="px-5 py-3.5 flex items-start justify-between gap-6"
                        >
                          <p className="text-sm leading-snug flex-1">
                            {reason.description}
                          </p>
                          <div className="flex items-center gap-2 shrink-0">
                            <DirectionTag direction={reason.direction} />
                            {reason.value !== null && (
                              <span className="type-mono text-muted-foreground text-xs">
                                {formatFeatureValue(reason.value)}
                              </span>
                            )}
                          </div>
                        </div>
                      ))}
                    </div>
                  </section>
                )}

                {/* Supporting Policy */}
                <section className="border border-border">
                  <div className="px-5 py-3 border-b border-border">
                    <h2 className="type-title">Supporting Policy</h2>
                  </div>
                  {analysis.policyGuidance.length > 0 ? (
                    <Accordion multiple defaultValue={["policy-0"]}>
                      <>
                        {analysis.policyGuidance.map((policy, i) => (
                          <AccordionItem
                            key={i}
                            value={`policy-${i}`}
                            className="border-b border-border last:border-b-0"
                          >
                            <AccordionTrigger className="px-5 text-sm font-medium hover:bg-muted/50 hover:no-underline">
                              {policy.document.split("/").pop() ??
                                policy.document}
                            </AccordionTrigger>
                            <AccordionContent className="px-5 pb-4">
                              <p className="type-body text-muted-foreground">
                                {policy.excerpt}
                              </p>
                              {policy.relevance != null && (
                                <p className="type-caption text-muted-foreground mt-2">
                                  Relevance score: {policy.relevance.toFixed(2)}
                                </p>
                              )}
                              <div className="mt-3 flex flex-wrap gap-2">
                                <Button
                                  type="button"
                                  variant="outline"
                                  size="sm"
                                  onClick={async () => {
                                    const citationText = [
                                      `Document: ${policy.document}`,
                                      `Excerpt: ${policy.excerpt}`,
                                      policy.relevance != null
                                        ? `Relevance: ${policy.relevance.toFixed(2)}`
                                        : null,
                                    ]
                                      .filter(Boolean)
                                      .join("\n");
                                    await navigator.clipboard.writeText(
                                      citationText,
                                    );
                                    toast.success("Citation copied");
                                  }}
                                >
                                  <Copy data-icon="inline-start" />
                                  Copy citation
                                </Button>
                                <Button
                                  type="button"
                                  variant="outline"
                                  size="sm"
                                  onClick={() => {
                                    setChatOpen(true);
                                    setQueuedChatQuestion(
                                      `What policy issue does ${
                                        policy.document.split("/").pop() ??
                                        policy.document
                                      } raise for claim ${claimId}?`,
                                    );
                                  }}
                                >
                                  Ask about this policy
                                </Button>
                              </div>
                            </AccordionContent>
                          </AccordionItem>
                        ))}
                      </>
                    </Accordion>
                  ) : (
                    <div className="px-5 py-4">
                      <p className="text-sm text-muted-foreground">
                        No matching policy documents found for this claim.
                      </p>
                    </div>
                  )}
                </section>

                {/* Analysis Summary */}
                <section className="border border-border">
                  <div className="px-5 py-3 border-b border-border">
                    <h2 className="type-title">Analysis Summary</h2>
                  </div>
                  <div className="px-5 py-4">
                    {analysis.narrative ? (
                      <p className="type-body leading-relaxed whitespace-pre-line text-pretty">
                        {analysis.narrative}
                      </p>
                    ) : (
                      <p className="text-sm text-muted-foreground">
                        No analysis summary available.
                      </p>
                    )}
                  </div>
                  {remediationChecklist.length > 0 && (
                    <>
                      <div className="px-5 py-3 border-t border-border">
                        <h3 className="type-title">Remediation Checklist</h3>
                      </div>
                      <ul className="px-5 py-4 space-y-2">
                        {remediationChecklist.map((item) => (
                          <li
                            key={item}
                            className="flex items-start gap-2 text-sm"
                          >
                            <span aria-hidden="true" className="text-muted-foreground">–</span>
                            <span>{item}</span>
                          </li>
                        ))}
                      </ul>
                    </>
                  )}
                </section>

                {analysis && (
                  <div className="flex justify-end">
                    <Button
                      variant="outline"
                      size="sm"
                      render={<Link href={`/claims/${claimId}/report`} />}
                      nativeButton={false}
                    >
                      <DownloadSimple data-icon="inline-start" />
                      Generate Review Report
                    </Button>
                  </div>
                )}

                {analysis.features &&
                  Object.keys(analysis.features).length > 0 && (
                    <section className="border border-border">
                    <Accordion defaultValue={[]} multiple={false}>
                        <AccordionItem
                          value="advanced-features"
                          className="border-b-0"
                        >
                          <AccordionTrigger className="px-5 py-3 type-title hover:no-underline">
                            Advanced model inputs
                          </AccordionTrigger>
                          <AccordionContent className="px-5 pb-4">
                            <div className="space-y-2">
                              {Object.entries(analysis.features).map(
                                ([key, value]) => (
                                  <div
                                    key={key}
                                    className="flex items-center justify-between gap-4 border-t border-border pt-2 first:border-t-0 first:pt-0"
                                  >
                                    <span className="type-mono text-xs text-muted-foreground">
                                      {key}
                                    </span>
                                    <span className="type-mono text-xs text-foreground">
                                      {value === null ? "—" : String(value)}
                                    </span>
                                  </div>
                                ),
                              )}
                            </div>
                          </AccordionContent>
                        </AccordionItem>
                      </Accordion>
                    </section>
                  )}

                {/* Policy Sources */}
                {analysis.policyCitations.length > 0 && (
                  <section className="border border-border">
                    <div className="px-5 py-3 border-b border-border">
                      <h2 className="type-title">Policy Sources</h2>
                    </div>
                    <ul className="px-5 py-3 space-y-2">
                      {analysis.policyCitations.map((citation, i) => (
                        <li
                          key={i}
                          className="flex items-center gap-2 text-sm text-muted-foreground"
                        >
                          <Files
                            className="size-3.5 shrink-0 text-muted-foreground"
                            aria-hidden="true"
                          />
                          <span>{citation}</span>
                        </li>
                      ))}
                    </ul>
                  </section>
                )}

                <section className="border border-border">
                  <Accordion defaultValue={[]} multiple={false}>
                    <AccordionItem value="feedback" className="border-b-0">
                      <AccordionTrigger className="px-5 py-3 type-title hover:no-underline">
                        Explanation Feedback
                      </AccordionTrigger>
                      <AccordionContent className="pb-4">
                        <FeedbackSection
                          key={feedbackQuery.dataUpdatedAt}
                          initialFeedback={feedbackQuery.data?.feedback ?? null}
                          isPending={feedbackMutation.isPending}
                          onSubmit={(payload) => feedbackMutation.mutate(payload)}
                        />
                      </AccordionContent>
                    </AccordionItem>
                  </Accordion>
                </section>

                <section className="border border-border">
                  <div className="px-5 py-3 border-b border-border">
                    <h2 className="type-title">Claim Activity</h2>
                  </div>
                  <div className="px-5 py-4 space-y-3">
                    {timelineQuery.data?.events?.length ? (
                      timelineQuery.data.events.map((event) => (
                        <div
                          key={`${event.eventType}-${event.createdAt}`}
                          className="pt-3 border-t border-dashed border-border/40 first:pt-0 first:border-t-0"
                        >
                          <p className="text-sm text-foreground">
                            {formatEventLabel(event)}
                          </p>
                          <p className="type-caption text-muted-foreground">
                            {formatDateTime(event.createdAt)}
                            {event.actorEmail ? ` • ${event.actorEmail}` : ""}
                          </p>
                        </div>
                      ))
                    ) : (
                      <p className="type-caption text-muted-foreground">
                        Activity will appear here after analysis, status
                        changes, feedback, and report generation.
                      </p>
                    )}
                  </div>
                </section>

                {/* FIX 2: Next high-risk button — after Policy Sources */}
                <div className="flex items-center justify-between pt-2 pb-4">
                  {nextHighRiskQuery.data ? (
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        router.push(`/claims/${nextHighRiskQuery.data}`)
                      }
                      aria-label="Navigate to next high-risk claim"
                    >
                      Next high-risk →
                    </Button>
                  ) : (
                    <Button
                      variant="outline"
                      size="sm"
                      disabled
                      aria-label="No more high-risk claims"
                    >
                      <span className="text-muted-foreground">
                        No more high-risk claims
                      </span>
                    </Button>
                  )}
                  <p className="type-caption text-muted-foreground">
                    Press{" "}
                    <kbd className="font-mono type-caption px-1 border border-border">
                      N
                    </kbd>{" "}
                    to jump
                  </p>
                </div>
              </>
            )}
          </div>
        </div>

        {/* RIGHT COLUMN — chat panel (hidden below lg breakpoint) */}
        <div className="hidden lg:flex w-95 xl:w-105 shrink-0 border-l border-border flex-col h-full">
          <ChatPanel
            claimId={claimId}
            analysis={analysis}
            queuedQuestion={queuedChatQuestion}
            onQueuedQuestionHandled={() => setQueuedChatQuestion(null)}
          />
        </div>

        {/* Mobile chat FAB — visible only below lg breakpoint */}
        <div className="lg:hidden">
          <button
            type="button"
            aria-label="Ask AI about this claim"
            onClick={() => setChatOpen(true)}
            className="fixed bottom-6 right-6 z-40 size-12 bg-foreground text-background flex items-center justify-center ring-1 ring-foreground/20 hover:ring-2 hover:ring-foreground/40 hover:bg-foreground/90 transition-all"
          >
            <ChatText className="size-5" />
          </button>
          <Sheet open={chatOpen} onOpenChange={setChatOpen}>
            <SheetContent
              side="right"
              showCloseButton
              className="w-full sm:max-w-100 p-0 flex flex-col"
            >
              <SheetHeader className="sr-only">
                <SheetTitle>Ask about this claim</SheetTitle>
                <SheetDescription>
                  Ask follow-up questions about the current claim analysis.
                </SheetDescription>
              </SheetHeader>
              <ChatPanel
                claimId={claimId}
                analysis={analysis}
                queuedQuestion={queuedChatQuestion}
                onQueuedQuestionHandled={() => setQueuedChatQuestion(null)}
              />
            </SheetContent>
          </Sheet>
        </div>
      </div>
    </AppShell>
  );
}
