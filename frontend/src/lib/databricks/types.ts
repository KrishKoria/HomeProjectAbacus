import type { FEATURE_COLUMNS } from "@/lib/contracts/claimops";

export type ClaimFeatureRow = Record<(typeof FEATURE_COLUMNS)[number], number | null>;

export interface ClaimAnalysisRequest {
  dataframe_split: {
    columns: string[];
    data: Array<Array<number | string | boolean | null>>;
  };
}

export interface TopReason {
  feature: string;
  value: number | null;
  shap_value: number;
  importance: number;
  description: string;
  direction: "increases_risk" | "decreases_risk" | "neutral";
}

export interface PolicyEvidence {
  document: string;
  excerpt: string;
  relevance: number | null;
}

export interface ClaimAnalysisResponse {
  claimId: string;
  features: Record<string, number | boolean | null>;
  riskScore: number;
  riskLevel: "low" | "medium" | "high";
  predictionLabel: number;
  topReasons: TopReason[];
  policyGuidance: PolicyEvidence[];
  narrative: string;
  policyCitations: string[];
  model: string;
  generatedAt: string;
}

export interface DatabricksStatus {
  oauth: boolean;
  sqlWarehouse: boolean;
  analysisEndpoint: boolean;
  vectorSearch: boolean;
  modelServing: boolean;
}
