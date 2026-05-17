"use client";

import Link from "next/link";
import { use, useEffect, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { ArrowLeft, Printer } from "@phosphor-icons/react";
import { Button } from "@/components/ui/button";
import { buildRemediationChecklist } from "@/lib/claims/remediation-checklist";
import type { ClaimAnalysisResponse } from "@/lib/databricks/types";

interface ClaimStatusPayload {
  claimId: string;
  reviewedAt: string | null;
  reviewedByEmail: string | null;
  reviewedById: string | null;
  status: string;
}

export default function ClaimReportPage({
  params,
}: {
  params: Promise<{ claimId: string }>;
}) {
  const { claimId } = use(params);

  const analysisQuery = useQuery({
    queryKey: ["claim-report-analysis", claimId],
    queryFn: async () => {
      const res = await fetch("/api/claims/analyze", {
        body: JSON.stringify({ claimId }),
        headers: { "Content-Type": "application/json" },
        method: "POST",
      });
      if (!res.ok) throw new Error("Failed to load analysis");
      return res.json() as Promise<ClaimAnalysisResponse>;
    },
  });

  const statusQuery = useQuery({
    queryKey: ["claim-report-status", claimId],
    queryFn: async () => {
      const res = await fetch(`/api/claims/${encodeURIComponent(claimId)}/status`);
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
  });

  useEffect(() => {
    fetch(`/api/claims/${encodeURIComponent(claimId)}/report-event`, {
      method: "POST",
    }).catch(() => {});
  }, [claimId]);

  if (analysisQuery.isLoading) {
    return <div className="p-6 text-sm text-muted-foreground">Loading report…</div>;
  }

  if (analysisQuery.isError || !analysisQuery.data) {
    return <div className="p-6 text-sm text-muted-foreground">Could not load report.</div>;
  }

  const analysis = analysisQuery.data;
  const status = statusQuery.data;
  const remediationChecklist = buildRemediationChecklist(analysis);

  return (
    <div className="mx-auto max-w-4xl space-y-6 p-6 print:p-0">
      <div className="flex items-center justify-between gap-3 print:hidden">
        <Button
          variant="ghost"
          size="sm"
          render={<Link href={`/claims/${claimId}`} />}
          nativeButton={false}
        >
          <ArrowLeft data-icon="inline-start" />
          Back to claim
        </Button>
        <Button type="button" onClick={() => window.print()}>
          <Printer data-icon="inline-start" />
          Print report
        </Button>
      </div>

      <header className="border border-border p-5">
        <h1 className="type-headline">Claim Review Report</h1>
        <div className="mt-4 grid gap-3 md:grid-cols-2">
          <ReportField label="Claim ID" value={analysis.claimId} mono />
          <ReportField label="Risk level" value={analysis.riskLevel} />
          <ReportField
            label="Risk score"
            value={`${Math.round(analysis.riskScore * 100)}%`}
          />
          <ReportField label="Status" value={status?.status ?? "new"} />
          <ReportField label="Reviewed by" value={status?.reviewedByEmail ?? "—"} />
          <ReportField
            label="Reviewed at"
            value={
              status?.reviewedAt
                ? new Date(status.reviewedAt).toLocaleString()
                : "—"
            }
          />
        </div>
      </header>

      <ReportSection title="Key Findings">
        <ul className="space-y-2">
          {analysis.topReasons.map((reason) => (
            <li
              key={`${reason.feature}-${reason.description}`}
              className="text-sm"
            >
              {reason.description}
            </li>
          ))}
        </ul>
      </ReportSection>

      <ReportSection title="Analysis Summary">
        <p className="text-sm leading-relaxed whitespace-pre-line">
          {analysis.narrative || "—"}
        </p>
      </ReportSection>

      {remediationChecklist.length > 0 && (
        <ReportSection title="Remediation Checklist">
          <ul className="space-y-2">
            {remediationChecklist.map((item) => (
              <li key={item} className="flex items-start gap-2 text-sm">
                <span aria-hidden="true">□</span>
                <span>{item}</span>
              </li>
            ))}
          </ul>
        </ReportSection>
      )}

      <ReportSection title="Supporting Policy">
        <div className="space-y-4">
          {analysis.policyGuidance.map((policy) => (
            <div
              key={`${policy.document}-${policy.excerpt}`}
              className="border-t border-border pt-3 first:border-t-0 first:pt-0"
            >
              <p className="text-sm font-medium">
                {policy.document.split("/").pop() ?? policy.document}
              </p>
              <p className="mt-1 text-sm text-muted-foreground">
                {policy.excerpt}
              </p>
              {policy.relevance != null && (
                <p className="mt-1 text-xs text-muted-foreground">
                  Relevance score: {policy.relevance.toFixed(2)}
                </p>
              )}
            </div>
          ))}
        </div>
      </ReportSection>

      <ReportSection title="Policy Sources">
        <ul className="space-y-2">
          {analysis.policyCitations.map((citation) => (
            <li key={citation} className="text-sm">
              {citation}
            </li>
          ))}
        </ul>
      </ReportSection>
    </div>
  );
}

function ReportField({
  label,
  mono = false,
  value,
}: {
  label: string;
  mono?: boolean;
  value: string;
}) {
  return (
    <div className="min-w-0">
      <p className="type-label text-muted-foreground">{label}</p>
      <p className={mono ? "font-mono text-sm text-foreground" : "text-sm text-foreground"}>
        {value}
      </p>
    </div>
  );
}

function ReportSection({
  children,
  title,
}: {
  children: ReactNode;
  title: string;
}) {
  return (
    <section className="border border-border p-5">
      <h2 className="type-title">{title}</h2>
      <div className="mt-3">{children}</div>
    </section>
  );
}
