"use client";

import { use, useCallback, useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { AppShell } from "@/components/app-shell";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { ClaimAnalysisResponse } from "@/lib/databricks/types";
import { ArrowLeft, Files, Warning } from "@phosphor-icons/react";

const riskBadgeClass: Record<string, string> = {
  high: "bg-risk-high-bg text-risk-high border-risk-high/20",
  medium: "bg-risk-medium-bg text-risk-medium border-risk-medium/20",
  low: "bg-risk-low-bg text-risk-low border-risk-low/20",
};

function directionClass(direction: string) {
  if (direction === "increases_risk") return "text-direction-up bg-direction-up-bg border-direction-up/20";
  if (direction === "decreases_risk") return "text-direction-down bg-direction-down-bg border-direction-down/20";
  return "text-muted-foreground bg-muted border-border";
}

function directionSymbol(direction: string) {
  if (direction === "increases_risk") return "+";
  if (direction === "decreases_risk") return "−";
  return "~";
}

function useCounter(target: number, duration: number) {
  const [value, setValue] = useState(0);
  const raf = useRef<number>(0);

  const animate = useCallback(() => {
    const start = performance.now();
    const step = (now: number) => {
      const elapsed = now - start;
      const progress = Math.min(elapsed / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      setValue(Math.round(eased * target));
      if (progress < 1) {
        raf.current = requestAnimationFrame(step);
      }
    };
    raf.current = requestAnimationFrame(step);
  }, [target, duration]);

  useEffect(() => {
    animate();
    return () => cancelAnimationFrame(raf.current);
  }, [animate]);

  return value;
}

export default function ClaimDetailPage({
  params,
}: {
  params: Promise<{ claimId: string }>;
}) {
  const { claimId } = use(params);
  const router = useRouter();
  const headingRef = useRef<HTMLHeadingElement>(null);

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

  useEffect(() => {
    if (analysisQuery.data) {
      headingRef.current?.focus();
    }
  }, [analysisQuery.data]);

  const analysis = analysisQuery.data;
  const riskLevel = analysis?.riskLevel ?? "low";
  const displayScore = useCounter(
    analysis ? Math.round(analysis.riskScore * 100) : 0,
    700
  );

  return (
    <AppShell>
      <div className="space-y-8 max-w-5xl">
        <div className="flex items-center gap-4">
          <Button
            variant="ghost"
            size="icon"
            onClick={() => router.push("/dashboard")}
          >
            <ArrowLeft />
          </Button>
          <h1
            ref={headingRef}
            tabIndex={-1}
            className="type-headline outline-none"
          >
            Claim{" "}
            <span className="type-mono">{claimId}</span>
          </h1>
        </div>

        {analysisQuery.isLoading ? (
          <div className="space-y-4">
            <Skeleton className="h-32 w-full" />
            <Skeleton className="h-48 w-full" />
          </div>
        ) : analysisQuery.isError ? (
          <section className="border border-border py-6 px-5">
            <div className="flex items-center gap-3">
              <Warning className="text-status-err" />
              <p className="text-sm text-muted-foreground flex-1">
                {analysisQuery.error.message}
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
        ) : analysis ? (
          <>
            <section className="border border-border py-6 px-5 space-y-4">
              <div className="flex items-start justify-between">
                <div className="space-y-2">
                  <div className="flex items-baseline gap-3">
                    <span className="type-display tabular-nums">
                      {displayScore}%
                    </span>
                    <Badge
                      variant="outline"
                      className={`text-xs uppercase ${riskBadgeClass[riskLevel]}`}
                    >
                      {riskLevel}
                    </Badge>
                  </div>
                  <Progress
                    value={displayScore}
                    className="h-2 w-full max-w-sm bg-muted"
                    style={
                      {
                        "--progress-indicator": `var(--risk-${riskLevel})`,
                      } as React.CSSProperties
                    }
                  />
                </div>
                <div className="text-xs text-muted-foreground space-y-1 text-right">
                  <p>
                    Generated{" "}
                    {analysis.generatedAt
                      ? new Date(analysis.generatedAt).toLocaleTimeString()
                      : "now"}
                  </p>
                  {analysis.model && <p>Model: {analysis.model}</p>}
                </div>
              </div>
            </section>

            <section className="border border-border">
              <div className="py-3 px-5 border-b border-border">
                <h2 className="type-title">Top Risk Factors</h2>
              </div>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="type-label">Feature</TableHead>
                    <TableHead className="type-label">Value</TableHead>
                    <TableHead className="type-label">Impact</TableHead>
                    <TableHead className="type-label">Direction</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {analysis.topReasons.map((reason) => (
                    <TableRow key={reason.feature}>
                      <TableCell className="font-medium">
                        {reason.feature}
                      </TableCell>
                      <TableCell className="type-mono">
                        {reason.value ?? "N/A"}
                      </TableCell>
                      <TableCell className="type-mono tabular-nums">
                        {reason.importance.toFixed(4)}
                      </TableCell>
                      <TableCell>
                        <span
                          className={`inline-flex items-center gap-1 px-2 py-0.5 text-xs font-medium border ${directionClass(
                            reason.direction
                          )}`}
                        >
                          {directionSymbol(reason.direction)}
                        </span>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </section>

            <Tabs defaultValue="policy" className="space-y-4">
              <TabsList>
                <TabsTrigger value="policy" className="text-sm">
                  Policy Guidance
                </TabsTrigger>
                <TabsTrigger value="narrative" className="text-sm">
                  Narrative
                </TabsTrigger>
              </TabsList>

              <TabsContent value="policy" className="border border-border">
                {analysis.policyGuidance.length > 0 ? (
                  <Accordion>
                    {analysis.policyGuidance.map((policy, i) => (
                      <AccordionItem
                        key={i}
                        value={`policy-${i}`}
                        className="border-b-border last:border-b-0"
                      >
                        <AccordionTrigger className="px-5 text-sm font-medium hover:bg-muted/50">
                          {policy.document.split("/").pop() ?? policy.document}
                        </AccordionTrigger>
                        <AccordionContent className="px-5 pb-4">
                          <p className="text-sm text-muted-foreground leading-relaxed">
                            {policy.excerpt}
                          </p>
                          {policy.relevance != null && (
                            <p className="text-xs text-muted-foreground mt-2">
                              Relevance: {policy.relevance.toFixed(2)}
                            </p>
                          )}
                        </AccordionContent>
                      </AccordionItem>
                    ))}
                  </Accordion>
                ) : (
                  <div className="py-6 px-5">
                    <p className="text-sm text-muted-foreground">
                      No policy guidance available for this claim.
                    </p>
                  </div>
                )}
              </TabsContent>

              <TabsContent value="narrative" className="border border-border py-6 px-5 space-y-4">
                {analysis.narrative ? (
                  <>
                    <p className="text-sm leading-relaxed whitespace-pre-line text-left text-pretty">
                      {analysis.narrative}
                    </p>
                    {analysis.policyCitations.length > 0 && (
                      <div className="border-t border-border pt-4 mt-4 space-y-2">
                        <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
                          Sources
                        </h3>
                        <ul className="space-y-1">
                          {analysis.policyCitations.map((citation, i) => (
                            <li key={i} className="flex items-start gap-2 text-xs text-muted-foreground">
                              <Files className="size-3 mt-0.5 shrink-0" />
                              <span>{citation}</span>
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}
                  </>
                ) : (
                  <p className="text-sm text-muted-foreground">
                    No narrative available.
                  </p>
                )}
              </TabsContent>
            </Tabs>
          </>
        ) : null}
      </div>
    </AppShell>
  );
}
