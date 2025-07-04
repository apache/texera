import { OperatorLink, OperatorPredicate } from "./workflow-common.interface";

/**
 * OperatorSuggestion is a lightweight subset of OperatorPredicate used in suggestions.
 */
export type OperatorSuggestion = Pick<
  OperatorPredicate,
  "operatorID" | "operatorType" | "customDisplayName" | "operatorProperties"
>;

export interface Changes {
  operatorsToAdd: OperatorSuggestion[];
  linksToAdd: OperatorLink[];
  operatorsToDelete: string[];
}

export type SuggestionType = "fix" | "improve";

export interface WorkflowSuggestion {
  suggestionID: string;
  suggestion: string;
  suggestionType: SuggestionType;
  changes: Changes;
}

export interface WorkflowSuggestionList {
  suggestions: WorkflowSuggestion[];
}

export interface WorkflowDataCleaningSuggestion {
  suggestionID: string;
  suggestion: string;
  details: string;
}

export interface WorkflowDataCleaningSuggestionList {
  suggestions: WorkflowDataCleaningSuggestion[];
}
