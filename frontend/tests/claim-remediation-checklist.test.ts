import { describe, expect, it } from "vitest";

import { buildRemediationChecklist } from "@/lib/claims/remediation-checklist";
import type { ClaimAnalysisResponse } from "@/lib/databricks/types";

function makeAnalysis(
  overrides: Partial<ClaimAnalysisResponse> = {},
): ClaimAnalysisResponse {
  return {
    claimId: "C0001",
    features: {},
    generatedAt: "2026-05-17T04:00:00.000Z",
    model: "claimops-analysis-v1",
    narrative: "",
    policyCitations: [],
    policyGuidance: [],
    predictionLabel: 1,
    riskLevel: "high",
    riskScore: 0.87,
    topReasons: [],
    ...overrides,
  };
}

describe("buildRemediationChecklist", () => {
  it("maps known higher-risk reasons to concrete analyst actions", () => {
    const checklist = buildRemediationChecklist(
      makeAnalysis({
        topReasons: [
          {
            description: "Procedure code is missing or incomplete.",
            direction: "increases_risk",
            feature: "is_procedure_missing",
            importance: 0.5,
            shap_value: 0.4,
            value: 1,
          },
          {
            description: "Diagnosis and procedure appear incompatible.",
            direction: "increases_risk",
            feature: "dx_px_compatible",
            importance: 0.4,
            shap_value: 0.3,
            value: 0,
          },
        ],
      }),
    );

    expect(checklist).toEqual(
      expect.arrayContaining([
        "Verify procedure code is present and complete.",
        "Confirm diagnosis-procedure compatibility before submission.",
      ]),
    );
  });

  it("falls back to generic QA actions when no mapping is available", () => {
    const checklist = buildRemediationChecklist(
      makeAnalysis({
        topReasons: [
          {
            description: "Unmapped signal.",
            direction: "increases_risk",
            feature: "custom_signal",
            importance: 0.2,
            shap_value: 0.1,
            value: 1,
          },
        ],
      }),
    );

    expect(checklist).toEqual([
      "Verify supporting documentation is attached before submission.",
      "Confirm code and billed amount consistency before submission.",
    ]);
  });

  it("ignores lower-risk and decreasing-risk analyses", () => {
    const checklist = buildRemediationChecklist(
      makeAnalysis({
        riskLevel: "low",
        topReasons: [
          {
            description: "Procedure code is missing or incomplete.",
            direction: "increases_risk",
            feature: "is_procedure_missing",
            importance: 0.5,
            shap_value: 0.4,
            value: 1,
          },
        ],
      }),
    );

    expect(checklist).toEqual([]);
  });
});
