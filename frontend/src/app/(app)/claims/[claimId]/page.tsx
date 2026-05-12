"use client";

import Link from "next/link";
import { use, useEffect, useRef, useState } from "react";
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
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
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
import type { ClaimAnalysisResponse } from "@/lib/databricks/types";
import { ArrowLeft, Files, Warning, PaperPlaneTilt, ChatText } from "@phosphor-icons/react";
import { RiskBadge, riskProgressColorMap } from "@/components/risk-badge";

// ─── Risk / Direction helpers ───────────────────────────────────────────────

function DirectionTag({ direction }: { direction: string }) {
  if (direction === "increases_risk")
    return (
      <span className="inline-flex items-center px-2 py-0.5 text-xs font-medium bg-direction-up-bg text-direction-up">
        Raises denial risk
      </span>
    );
  if (direction === "decreases_risk")
    return (
      <span className="inline-flex items-center px-2 py-0.5 text-xs font-medium bg-direction-down-bg text-direction-down">
        Lowers denial risk
      </span>
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

// ─── Chat panel ──────────────────────────────────────────────────────────────

interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

function ChatPanel({
  claimId,
  analysis,
}: {
  claimId: string;
  analysis: ClaimAnalysisResponse | undefined;
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
            analysis.riskLevel.charAt(0).toUpperCase() + analysis.riskLevel.slice(1)
          } denial risk. Ask me anything about it.`,
        }
      : null;
  const visibleMessages = openingMessage ? [openingMessage, ...messages] : messages;
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

  async function sendMessage() {
    const text = input.trim();
    if (!text || isWaiting || !analysis) return;
    setInput("");
    const userMsg: ChatMessage = { role: "user", content: text };
    const nextMessages = [...visibleMessages, userMsg];
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
        { role: "assistant", content: data.reply ?? "Sorry, I couldn't get a response." },
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

  function onKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  }

  return (
    <div className="flex flex-col h-full">
      <div className="px-5 py-3 border-b border-border shrink-0">
        <div className="flex items-center gap-2">
          <ChatText className="size-4 text-muted-foreground" aria-hidden="true" />
          <span className="type-title">Ask about this claim</span>
        </div>
        <p className="type-caption text-muted-foreground mt-0.5">
          Press <kbd className="font-mono text-[10px] px-1 border border-border">C</kbd> to focus
        </p>
      </div>

      <div ref={threadRef} className="flex-1 overflow-y-auto px-4 py-4 space-y-4" aria-live="polite" aria-label="Chat messages">
        {!analysis && (
          <p className="type-caption text-muted-foreground text-center mt-8">
            Waiting for analysis…
          </p>
        )}
        {visibleMessages.map((msg, i) => (
          <div
            key={i}
            className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
          >
            <div
              className={`max-w-[85%] px-3 py-2 text-sm leading-relaxed ${
                msg.role === "user"
                  ? "bg-primary text-primary-foreground"
                  : "bg-muted text-foreground border border-border"
              }`}
            >
              {msg.content}
            </div>
          </div>
        ))}
        {isWaiting && (
          <div className="flex justify-start">
            <div className="bg-muted border border-border px-3 py-2">
              <div className="flex items-center gap-1">
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
            className="flex-1 resize-none bg-transparent border border-border px-3 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring disabled:opacity-50 min-h-[2.5rem] max-h-28"
          />
          <Button
            size="icon"
            onClick={sendMessage}
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
      onValueChange={(v) => v && mutation.mutate(v)}
      disabled={mutation.isPending}
    >
      <SelectTrigger className="w-36 h-10 text-xs" aria-label="Claim status">
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
      const res = await fetch(`/api/claims/${encodeURIComponent(claimId)}/status`);
      if (!res.ok) return "new";
      const data = (await res.json()) as { status: string };
      return data.status ?? "new";
    },
    staleTime: 5 * 60 * 1000,
  });

  useEffect(() => {
    if (analysisQuery.data) {
      queryClient.invalidateQueries({ queryKey: ["claims"] });
      headingRef.current?.focus();
    }
  }, [analysisQuery.data, queryClient]);

  const analysis = analysisQuery.data;
  const riskLevel = analysis?.riskLevel ?? "low";
  const displayScore = analysis ? Math.round(analysis.riskScore * 100) : 0;

  const breadcrumb = (
    <>
      <Link
        href="/claims"
        className="hover:text-foreground transition-colors"
      >
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
                  claimId={claimId}
                  initialStatus={claimStatusQuery.data ?? "new"}
                />
              )}
            </div>

            {/* Loading */}
            {analysisQuery.isLoading && (
              <div className="space-y-4">
                <div className="border border-border p-5 space-y-3">
                  <Skeleton className="h-12 w-32" />
                  <Skeleton className="h-2 w-48" />
                </div>
                <div className="border border-border">
                  <div className="px-5 py-3 border-b border-border">
                    <Skeleton className="h-4 w-28" />
                  </div>
                  {Array.from({ length: 4 }).map((_, i) => (
                    <div key={i} className="px-5 py-3 border-b border-border last:border-b-0">
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

            {/* Error */}
            {analysisQuery.isError && (
              <section className="border border-border py-5 px-5">
            <div className="flex items-center gap-3">
              <Warning className="size-4 text-status-err shrink-0" aria-hidden="true" />
                  <p className="type-body text-muted-foreground flex-1">
                    Could not analyze this claim. Please try again.
                  </p>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => analysisQuery.refetch()}
                  >
                    Retry
                  </Button>
                </div>
              </section>
            )}

            {/* Analysis */}
            {analysis && (
              <>
                {/* Risk score */}
                <section className="border border-border p-5 space-y-3">
                  <div className="flex items-start justify-between gap-4">
                    <div className="space-y-2">
                      <div className="flex items-baseline gap-3">
                        <span className="type-display tabular-nums">{displayScore}%</span>
                        <RiskBadge level={riskLevel} className="uppercase tracking-wider" />
                      </div>
                      <Progress
                        value={displayScore}
                        className="h-1.5 w-48 bg-muted"
                        style={
                          {
                            "--progress-indicator": riskProgressColorMap[riskLevel],
                          } as React.CSSProperties
                        }
                      />
                    </div>
                    <div className="text-xs text-muted-foreground space-y-1 text-right shrink-0">
                      {analysis.generatedAt && (
                        <p>
                          {new Date(analysis.generatedAt).toLocaleTimeString(undefined, {
                            hour: "2-digit",
                            minute: "2-digit",
                          })}
                        </p>
                      )}
                      {analysis.model && (
                        <p className="type-mono opacity-60">{analysis.model}</p>
                      )}
                    </div>
                  </div>
                </section>

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
                          <p className="text-sm leading-snug flex-1">{reason.description}</p>
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
                      <>{analysis.policyGuidance.map((policy, i) => (
                        <AccordionItem
                          key={i}
                          value={`policy-${i}`}
                          className="border-b border-border last:border-b-0"
                        >
                          <AccordionTrigger className="px-5 text-sm font-medium hover:bg-muted/50 hover:no-underline">
                            {policy.document.split("/").pop() ?? policy.document}
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
                          </AccordionContent>
                        </AccordionItem>
                      ))}</>
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
                </section>

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
                          <Files className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
                          <span>{citation}</span>
                        </li>
                      ))}
                    </ul>
                  </section>
                )}
              </>
            )}
          </div>
        </div>

        {/* RIGHT COLUMN — chat panel (hidden below lg breakpoint) */}
        <div className="hidden lg:flex w-[380px] xl:w-[420px] shrink-0 border-l border-border flex-col h-full">
          <ChatPanel claimId={claimId} analysis={analysis} />
        </div>

        {/* Mobile chat FAB — visible only below lg breakpoint */}
        <div className="lg:hidden">
          <button
            type="button"
            aria-label="Ask AI about this claim"
            onClick={() => setChatOpen(true)}
            className="fixed bottom-6 right-6 z-40 size-12 bg-foreground text-background flex items-center justify-center shadow-lg hover:bg-foreground/90 transition-colors"
          >
            <ChatText className="size-5" />
          </button>
          <Sheet open={chatOpen} onOpenChange={setChatOpen}>
            <SheetContent side="right" showCloseButton className="w-full sm:max-w-[400px] p-0 flex flex-col">
              <SheetHeader className="sr-only">
                <SheetTitle>Ask about this claim</SheetTitle>
                <SheetDescription>
                  Ask follow-up questions about the current claim analysis.
                </SheetDescription>
              </SheetHeader>
              <ChatPanel claimId={claimId} analysis={analysis} />
            </SheetContent>
          </Sheet>
        </div>
      </div>
    </AppShell>
  );
}
