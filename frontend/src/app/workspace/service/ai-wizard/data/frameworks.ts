/**
 * Scientific data analysis frameworks. Ported from TexeraHackathon.
 * The Custom framework is editable freeform text.
 */

import { ScientificFramework } from "../types";

export interface FrameworkDefinition {
  name: ScientificFramework;
  description: string;
  phases: string[];
  prompt: string;
}

export const FRAMEWORKS: Record<ScientificFramework, FrameworkDefinition> = {
  "CRISP-DM": {
    name: "CRISP-DM",
    description: "Cross-Industry Standard Process for Data Mining",
    phases: ["Business Understanding", "Data Understanding", "Data Preparation", "Modeling", "Evaluation", "Deployment"],
    prompt: `Follow the CRISP-DM methodology:
1. **Data Understanding**: Start with exploratory data analysis (EDA) - use visualization operators to understand distributions, correlations, missing values
2. **Data Preparation**: Clean data - handle nulls, remove duplicates, filter outliers, select relevant columns
3. **Modeling**: Apply appropriate algorithms based on the analysis goal
4. **Evaluation**: Include visualization operators to evaluate results
5. Structure the workflow with clear stages following this order`,
  },
  SEMMA: {
    name: "SEMMA",
    description: "Sample, Explore, Modify, Model, Assess",
    phases: ["Sample", "Explore", "Modify", "Model", "Assess"],
    prompt: `Follow the SEMMA methodology:
1. **Sample**: Use Limit operator to sample representative data subset
2. **Explore**: Use visualization operators (ScatterMatrix, BarChart, etc.) for EDA
3. **Modify**: Transform and prepare data (Filter, TypeCasting, Projection, Aggregate)
4. **Model**: Apply modeling operators (if applicable)
5. **Assess**: Visualize and evaluate results
6. Structure the workflow with these clear phases`,
  },
  KDD: {
    name: "KDD",
    description: "Knowledge Discovery in Databases",
    phases: ["Selection", "Preprocessing", "Transformation", "Data Mining", "Interpretation/Evaluation"],
    prompt: `Follow the KDD process:
1. **Selection**: Start with data source operator, optionally filter to select relevant subset
2. **Preprocessing**: Clean and prepare data (handle missing values, remove noise)
3. **Transformation**: Transform data into suitable format (aggregations, type conversions, feature engineering)
4. **Data Mining**: Apply pattern discovery or modeling operators
5. **Interpretation/Evaluation**: Visualize and interpret results
6. Structure operators in this sequential order`,
  },
  Custom: {
    name: "Custom",
    description: "Define your own methodology and domain knowledge",
    phases: [],
    prompt: `Follow this custom methodology:
1. **Step 1**: Describe the first phase of your analysis.
2. **Step 2**: Describe the second phase of your analysis.
3. **Domain knowledge**: Add any constraints, biases, or domain-specific guidance here.
4. Structure the workflow according to this methodology.`,
  },
};

export function getFrameworkPrompt(framework: ScientificFramework): string {
  return FRAMEWORKS[framework].prompt;
}
