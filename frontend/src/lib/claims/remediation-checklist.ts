import type { ClaimAnalysisResponse } from "@/lib/databricks/types";

const GENERIC_ACTIONS = [
  "Verify supporting documentation is attached before submission.",
  "Confirm code and billed amount consistency before submission.",
] as const;

const FEATURE_ACTIONS: Record<string, string[]> = {
  amount_to_benchmark_ratio: [
    "Check billed amount against the expected benchmark for this service.",
    "Confirm any outlier amount has supporting documentation before submission.",
  ],
  dx_px_compatible: [
    "Confirm diagnosis-procedure compatibility before submission.",
    "Update diagnosis or procedure coding if the documented encounter does not support the pairing.",
  ],
  is_procedure_missing: [
    "Verify procedure code is present and complete.",
    "Add the missing procedure detail before the claim is submitted.",
  ],
  provider_30d_denial_rate: [
    "Review whether this provider has a recurring denial pattern for the same service.",
    "Escalate to the coding or billing lead if the provider trend needs a second review.",
  ],
};

const DESCRIPTION_ACTIONS = [
  {
    actions: FEATURE_ACTIONS.amount_to_benchmark_ratio,
    pattern: /amount|benchmark|billed/i,
  },
  {
    actions: FEATURE_ACTIONS.dx_px_compatible,
    pattern: /diagnosis|procedure|compatible/i,
  },
  {
    actions: FEATURE_ACTIONS.is_procedure_missing,
    pattern: /procedure code.*missing|missing.*procedure/i,
  },
] as const;

export function buildRemediationChecklist(analysis: ClaimAnalysisResponse): string[] {
  if (analysis.riskLevel === "low") {
    return [];
  }

  const actions = new Set<string>();

  for (const reason of analysis.topReasons) {
    if (reason.direction !== "increases_risk") {
      continue;
    }

    const mappedActions =
      FEATURE_ACTIONS[reason.feature] ??
      DESCRIPTION_ACTIONS.find((entry) => entry.pattern.test(reason.description))?.actions ??
      null;

    if (!mappedActions) {
      continue;
    }

    for (const action of mappedActions) {
      actions.add(action);
      if (actions.size >= 6) {
        return [...actions];
      }
    }
  }

  return actions.size > 0 ? [...actions] : [...GENERIC_ACTIONS];
}
