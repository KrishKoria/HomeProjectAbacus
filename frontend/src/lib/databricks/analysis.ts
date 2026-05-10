import { databricksFetch } from "@/lib/databricks/client";
import { FEATURE_COLUMNS } from "@/lib/contracts/claimops";
import type { ClaimAnalysisResponse } from "@/lib/databricks/types";
import { env } from "@/lib/server/env";

interface ServingEndpointResponse {
  predictions: [ClaimAnalysisResponse];
}

export async function analyzeClaim(
  claimId: string,
  features: Record<string, number | null>,
): Promise<
  | { ok: true; data: ClaimAnalysisResponse }
  | { ok: false; status: number; message: string }
> {
  console.log("[analysis] claimId=%s features sample", claimId, {
    is_procedure_missing: features.is_procedure_missing,
    diagnosis_count: features.diagnosis_count,
    billed_vs_avg_cost: features.billed_vs_avg_cost,
    missing_fields_count: features.missing_fields_count,
    provider_claim_count_30d: features.provider_claim_count_30d,
  });

  const payload = {
    dataframe_split: {
      columns: [...FEATURE_COLUMNS, "claim_id"],
      data: [[...FEATURE_COLUMNS.map((col) => features[col] ?? null), claimId]],
    },
  };

  const result = await databricksFetch<ServingEndpointResponse>(
    `/serving-endpoints/${env.CLAIMOPS_ANALYSIS_ENDPOINT}/invocations`,
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
  );

  if (!result.ok) return result;

  const prediction = result.data.predictions[0];
  return { ok: true, data: prediction };
}
