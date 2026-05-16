/**
 * White-box guardrails enforced regardless of methodology text. Ported from
 * TexeraHackathon. Design-doc §4.2: guardrails are validator-checkable rules.
 */

import { Guardrail } from "../types";

export const DEFAULT_GUARDRAILS: Guardrail[] = [
  {
    id: "data-validation",
    name: "Data Validation",
    description: "Always validate input data quality and schema before processing",
    enabled: true,
  },
  {
    id: "error-handling",
    name: "Error Handling",
    description: "Include proper error handling operators to catch data issues",
    enabled: true,
  },
  {
    id: "sample-first",
    name: "Sample Data First",
    description: "Use Limit operator to sample data before expensive operations",
    enabled: true,
  },
  {
    id: "type-safety",
    name: "Type Safety",
    description: "Ensure column types are properly cast before operations",
    enabled: true,
  },
  {
    id: "visualization",
    name: "Visualization",
    description: "Include visualization operators to inspect intermediate results",
    enabled: true,
  },
  {
    id: "reproducibility",
    name: "Reproducibility",
    description: "Set random seeds for random operations (Split, sampling, etc.)",
    enabled: true,
  },
  {
    id: "null-handling",
    name: "Null Value Handling",
    description: "Filter or handle null values before aggregations",
    enabled: true,
  },
  {
    id: "performance",
    name: "Performance Optimization",
    description: "Apply filters early in the pipeline to reduce data volume",
    enabled: true,
  },
  {
    id: "train-test-split",
    name: "Mandatory Train/Test Split",
    description:
      "For Predictive Modeling, ALWAYS insert a Split operator (e.g., 80/20) BEFORE any modeling operator. Train on the training partition only.",
    enabled: true,
  },
  {
    id: "data-leakage",
    name: "Prevent Data Leakage",
    description:
      "Never apply transformations fit on the full dataset to the test partition. Any sampling, scaling, or feature engineering that uses dataset statistics must be done AFTER the train/test split, fit on training data only.",
    enabled: true,
  },
  {
    id: "evaluation",
    name: "Mandatory Evaluation",
    description:
      "For Predictive Modeling, ALWAYS include at least one evaluation operator (e.g., Scatterplot of predicted vs. actual, or an aggregate of error metrics) on the test-set predictions.",
    enabled: true,
  },
  {
    id: "no-synthetic-data",
    name: "No Synthetic Data by Default",
    description:
      "Do NOT introduce synthetic samples (e.g., SMOTE-style oversampling, generated rows) unless the user explicitly requests it. Class imbalance should be reported, not silently fixed.",
    enabled: true,
  },
];

export function getGuardrailsPrompt(guardrails: Guardrail[]): string {
  const enabled = guardrails.filter(g => g.enabled);
  if (enabled.length === 0) return "";
  const rules = enabled.map((g, idx) => `${idx + 1}. **${g.name}**: ${g.description}`).join("\n");
  return `\n\n## Guardrails\nThe following guardrails MUST be followed:\n${rules}`;
}
