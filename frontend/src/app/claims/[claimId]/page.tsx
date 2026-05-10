"use client";

import { use, useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { AppShell } from "@/components/app-shell";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import type { ClaimAnalysisResponse } from "@/lib/databricks/types";
import { ArrowLeft, Warning } from "@phosphor-icons/react";

export default function ClaimDetailPage({
  params,
}: {
  params: Promise<{ claimId: string }>;
}) {
  const { claimId } = use(params);
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

  const riskBadgeVariant =
    analysisQuery.data?.riskLevel === "high"
      ? "destructive"
      : analysisQuery.data?.riskLevel === "medium"
        ? "secondary"
        : "outline";

  return (
    <AppShell>
      <div className="space-y-6 max-w-4xl">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => router.push("/dashboard")}>
            <ArrowLeft />
          </Button>
          <h1 className="text-lg font-semibold tracking-tight">Claim {claimId}</h1>
        </div>

        {analysisQuery.isLoading ? (
          <div className="space-y-4">
            <Skeleton className="h-24 w-full" />
            <Skeleton className="h-48 w-full" />
          </div>
        ) : analysisQuery.isError ? (
          <Card>
            <CardContent className="flex items-center gap-3 py-6">
              <Warning />
              <p className="text-sm text-muted-foreground">
                {analysisQuery.error.message}
              </p>
            </CardContent>
          </Card>
        ) : analysisQuery.data ? (
          <>
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Risk Assessment</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="flex items-center gap-4">
                  <div className="text-4xl font-bold tracking-tight">
                    {(analysisQuery.data.riskScore * 100).toFixed(0)}%
                  </div>
                  <Badge variant={riskBadgeVariant} className="text-xs uppercase">
                    {analysisQuery.data.riskLevel}
                  </Badge>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="text-base">Top Risk Factors</CardTitle>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Feature</TableHead>
                      <TableHead>Value</TableHead>
                      <TableHead>Impact</TableHead>
                      <TableHead>Direction</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {analysisQuery.data.topReasons.map((reason) => (
                      <TableRow key={reason.feature}>
                        <TableCell className="font-medium">{reason.feature}</TableCell>
                        <TableCell>{reason.value ?? "N/A"}</TableCell>
                        <TableCell>{reason.importance.toFixed(4)}</TableCell>
                        <TableCell>
                          <Badge
                            variant="outline"
                            className={
                              reason.direction === "increases_risk"
                                ? "text-red-600 border-red-200"
                                : "text-green-600 border-green-200"
                            }
                          >
                            {reason.direction === "increases_risk" ? "+" : "-"}
                          </Badge>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>

            {analysisQuery.data.policyGuidance.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Policy Guidance</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  {analysisQuery.data.policyGuidance.map((policy, i) => (
                    <div key={i} className="text-sm space-y-1">
                      <p className="font-medium">{policy.document}</p>
                      <p className="text-muted-foreground line-clamp-3">
                        {policy.excerpt}
                      </p>
                    </div>
                  ))}
                </CardContent>
              </Card>
            )}

            {analysisQuery.data.narrative && (
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Narrative</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    {analysisQuery.data.narrative}
                  </p>
                </CardContent>
              </Card>
            )}
          </>
        ) : null}
      </div>
    </AppShell>
  );
}
