"use client";

import { useEffect, useRef, useState } from "react";
import { ChatText, PaperPlaneTilt } from "@phosphor-icons/react";
import { Button } from "@/components/ui/button";
import type { ClaimAnalysisResponse } from "@/lib/databricks/types";
import type { ChatMessage } from "@/lib/claims/types";

export function ChatPanel({
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

  const qaPairs: Array<{ question: string; answer: string | null }> = [];
  for (let i = 0; i < messages.length; i++) {
    if (messages[i].role === "user") {
      const next = messages[i + 1];
      qaPairs.push({
        question: messages[i].content,
        answer: next?.role === "assistant" ? next.content : null,
      });
      if (next?.role === "assistant") i++;
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

        {qaPairs.map((pair, i) => (
          <div
            key={i}
            className={
              i < qaPairs.length - 1 || isWaiting
                ? "pb-6 border-b border-border/40"
                : "pb-2"
            }
          >
            <div className="flex items-start gap-2.5 mb-3">
              <div
                className="w-0.5 self-stretch bg-foreground/20 shrink-0 mt-0.5"
                aria-hidden="true"
              />
              <p className="type-label text-foreground leading-snug">
                {pair.question}
              </p>
            </div>
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
