/**
 * Types for the AI Wizard. Ported from TexeraHackathon/src/types/wizard.ts and
 * extended with DataProfile (design-doc §4.2).
 */

export type AnalysisGoal = "EDA" | "Predictive Modeling" | "Data Cleaning" | "NLP" | "Custom";

export type DataSource = "Existing Dataset" | "dkNET Dataset";

export type ScientificFramework = "CRISP-DM" | "SEMMA" | "KDD" | "Custom";

export interface DknetDataset {
  id: string;
  name: string;
  description: string;
  fileName: string;
  schema: string;
  profile?: DataProfile;
}

export interface Guardrail {
  id: string;
  name: string;
  description: string;
  enabled: boolean;
}

export type ColumnDtype = "int" | "float" | "str" | "bool" | "date";

export interface ColumnProfile {
  name: string;
  dtype: ColumnDtype;
  nullRate: number;
  uniqueCount: number;
  sampleValues: string[];
}

export interface DataProfile {
  rowCount: number;
  columns: ColumnProfile[];
  source: "csv-upload" | "dknet-prebaked" | "unavailable";
}

export interface WizardState {
  step: number;
  analysisGoal?: AnalysisGoal;
  customAnalysisGoal?: string;
  dataSource?: DataSource;
  framework?: ScientificFramework;
  frameworkPrompt?: string;
  guardrails: Guardrail[];
  /** When dataSource === "Existing Dataset": the Texera-resolved path
   *  (e.g. "/<owner>/<dataset>/v1/<file>.csv") chosen via DatasetSelectionModal. */
  existingDatasetPath?: string;
  dknetDataset?: DknetDataset;
  dataProfile?: DataProfile;
  /** LLM model id passed to /api/chat/completions (e.g. claude-haiku-4.5). */
  model?: string;
}

export interface AttemptLog {
  attempt: number;
  errorCount: number;
  errors: string[];
}

export interface ValidationError {
  field: string;
  message: string;
}

export interface ValidationResult {
  isValid: boolean;
  errors: ValidationError[];
  warnings?: string[];
}
