import { HttpClient, HttpHeaders } from "@angular/common/http";
import { Injectable, OnDestroy } from "@angular/core";
import { Observable, of, BehaviorSubject, pipe, timer, delay } from "rxjs";
import { catchError, map, tap, finalize } from "rxjs/operators";
import { AppSettings } from "../../../common/app-setting";
import { Workflow } from "../../../common/type/workflow";
import { ExecutionStateInfo } from "../../types/execute-workflow.interface";
import { WorkflowDataCleaningSuggestionList, WorkflowSuggestionList } from "../../types/workflow-suggestion.interface";
import { v4 as uuid } from "uuid";
import { CompilationStateInfo, SchemaAttribute } from "../../types/workflow-compiling.interface";
import { TableProfile } from "../../../common/type/proto/edu/uci/ics/amber/engine/architecture/worker/tableprofile";

// endpoint for workflow suggestions
export const WORKFLOW_SUGGESTION_ENDPOINT = "workflow-suggestion";
// new endpoint for data cleaning suggestions
export const DATA_CLEANING_SUGGESTION_ENDPOINT = "data-cleaning-suggestion";

// Define the request interface if not already globally available
export interface TableProfileSuggestionRequest {
  focusingOperatorID: string;
  tableSchema: ReadonlyArray<SchemaAttribute>;
  tableProfile: TableProfile;
  targetColumnName: string;
}

/**
 * WorkflowSuggestionService is responsible for communicating with the backend suggestion service.
 * It gathers the necessary data (workflow, compilation state, and result data) and sends it to
 * the backend to generate workflow suggestions.
 */
@Injectable({
  providedIn: "root",
})
export class WorkflowSuggestionService implements OnDestroy {
  // Stream that indicates whether a preview is currently active - initialized to false
  private previewActiveStream = new BehaviorSubject<boolean>(false);
  // Stream that always holds the latest list of suggestions returned from the backend.
  private suggestionsListSubject = new BehaviorSubject<WorkflowSuggestionList>({ suggestions: [] });
  // Stream indicating whether a request is in flight so that components can show loading states.
  private suggestionsLoadingSubject = new BehaviorSubject<boolean>(false);
  // Flag to ignore workflow changes during preview activation/deactivation
  private mock = false; // Set to true to enable mock mode

  constructor(private httpClient: HttpClient) {
    // Ensure preview is false on initial load
    this.resetPreviewState();
  }

  ngOnDestroy(): void {
    // Make sure preview state is reset when service is destroyed
    this.resetPreviewState();
  }

  /**
   * Observable stream of the latest workflow suggestion list.  Components that want to
   * reactively display suggestions (for example the SuggestionFrameComponent) should subscribe
   * to this stream instead of (or in addition to) directly calling getSuggestions().  Whenever
   * getSuggestions() successfully receives a response from the backend this stream will emit
   * the same suggestion list so that every interested component stays in sync.
   */
  public getSuggestionsListStream(): Observable<WorkflowSuggestionList> {
    return this.suggestionsListSubject.asObservable();
  }

  /**
   * Observable stream indicating whether suggestions are currently being fetched.
   */
  public getLoadingStream(): Observable<boolean> {
    return this.suggestionsLoadingSubject.asObservable();
  }

  /**
   * Requests workflow suggestions from the backend service.
   * This method gathers the current workflow state, compilation information,
   * and result data, then sends it to the backend to generate suggestions.
   *
   * @returns Observable of workflow suggestions
   */
  public getSuggestions(
    workflow: Workflow,
    compilationState: CompilationStateInfo,
    executionState: ExecutionStateInfo,
    intention: string,
    focusingOperatorIDs: readonly string[],
    operatorIDToTableSchemaMap: Record<string, ReadonlyArray<SchemaAttribute>>
  ): Observable<WorkflowSuggestionList> {
    // indicate loading started
    this.suggestionsLoadingSubject.next(true);

    // Helper to mark request finished
    const done = () => this.suggestionsLoadingSubject.next(false);

    // Skip if preview is active
    if (this.previewActiveStream.getValue()) {
      return of({ suggestions: [] }).pipe(
        tap(list => this.suggestionsListSubject.next(list)),
        finalize(done)
      );
    }

    if (this.mock) {
      return of(this.MOCK_SUGGESTIONS).pipe(
        tap(list => this.suggestionsListSubject.next(list)),
        finalize(done)
      );
    }

    return this.httpClient
      .post<WorkflowSuggestionList>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_SUGGESTION_ENDPOINT}`, {
        workflow: JSON.stringify(workflow),
        compilationState: compilationState,
        executionState: executionState,
        intention: intention,
        focusingOperatorIDs: focusingOperatorIDs,
        operatorIDToTableSchemaMap: operatorIDToTableSchemaMap,
      })
      .pipe(
        map(suggestionList => {
          suggestionList.suggestions.forEach(suggestion => {
            suggestion.suggestionID = `suggestion-${uuid()}`;
          });
          return suggestionList;
        }),
        // Publish the suggestion list so that other components (e.g. the suggestion frame)
        // can react to the new data even if they did not initiate the request.
        tap(suggestionList => this.suggestionsListSubject.next(suggestionList)),
        catchError((error: unknown) => {
          console.error("Error getting workflow suggestions:", error);
          // publish empty list on error
          this.suggestionsListSubject.next({ suggestions: [] });
          return of({ suggestions: [] });
        }),
        finalize(done)
      );
  }

  /**
   * Get an observable stream indicating whether a preview is active.
   * Components can subscribe to this to know when to skip certain operations.
   *
   * @returns Observable boolean indicating if a preview is active
   */
  public getPreviewActiveStream(): Observable<boolean> {
    return this.previewActiveStream.asObservable();
  }

  /**
   * Set whether a preview is currently active.
   * This will notify all subscribers to the preview active stream.
   *
   * @param isActive Whether a preview is currently active
   */
  public setPreviewActive(isActive: boolean): void {
    console.log(`WorkflowSuggestionService: Setting preview active to ${isActive}`);

    // Set the preview state
    this.previewActiveStream.next(isActive);
  }

  /**
   * Reset the preview state to false.
   * This should be called when navigating away from the workspace or
   * when the component is destroyed.
   */
  public resetPreviewState(): void {
    this.setPreviewActive(false);
  }

  /** Remove a suggestion with given ID from current list and notify subscribers. */
  public removeSuggestionById(id: string): void {
    const current = this.suggestionsListSubject.getValue();
    const newList = { suggestions: current.suggestions.filter(s => s.suggestionID !== id) };
    this.suggestionsListSubject.next(newList);
  }

  /**
   * Requests data cleaning suggestions from the backend service based on table profile and target column.
   * @param focusingOperatorID
   * @param tableSchema
   * @param tableProfile The complete table profile.
   * @param targetColumnName The name of the column for which to get suggestions.
   * @returns Observable of SuggestionList
   */
  public getDataCleaningSuggestions(
    focusingOperatorID: string,
    tableSchema: ReadonlyArray<SchemaAttribute>,
    tableProfile: TableProfile,
    targetColumnName: string
  ): Observable<WorkflowDataCleaningSuggestionList> {
    const requestPayload: TableProfileSuggestionRequest = {
      focusingOperatorID: focusingOperatorID,
      tableSchema: tableSchema,
      tableProfile: tableProfile,
      targetColumnName: targetColumnName,
    };

    return this.httpClient
      .post<WorkflowDataCleaningSuggestionList>(
        `${AppSettings.getApiEndpoint()}/${DATA_CLEANING_SUGGESTION_ENDPOINT}`,
        requestPayload
      )
      .pipe(
        map(suggestionList => {
          // Ensure suggestionIDs are unique if backend doesn't guarantee it
          suggestionList.suggestions.forEach(suggestion => {
            if (!suggestion.suggestionID) {
              suggestion.suggestionID = `dcsuggestion-${uuid()}`;
            }
          });
          return suggestionList;
        }),
        catchError((error: unknown) => {
          console.error("Error getting data cleaning suggestions:", error);
          return of({ suggestions: [] } as WorkflowDataCleaningSuggestionList); // Return empty list on error
        })
      );
  }

  MOCK_SUGGESTIONS: WorkflowSuggestionList = {
    suggestions: [
      {
        suggestionID: "suggestion-0",
        suggestion: "Add aggregation to compute summary statistics for tweet data.",
        suggestionType: "improve",
        changes: {
          operatorsToAdd: [
            {
              operatorType: "Aggregate",
              operatorID: "Aggregate-operator-new-1",
              operatorProperties: {
                aggregations: [
                  {
                    aggFunction: "sum",
                    attribute: "favorite_count",
                    "result attribute": "total_favorite_count",
                  },
                  {
                    aggFunction: "average",
                    attribute: "retweet_count",
                    "result attribute": "average_retweet_count",
                  },
                ],
                groupByKeys: ["create_at_month"],
              },
              customDisplayName: "Aggregate Tweet Data",
            },
          ],
          linksToAdd: [
            {
              linkID: "link-8af0915c-6ccc-439a-bf05-1b39cefdbbfd",
              source: {
                operatorID: "Sort-operator-3985eaf1-5af2-4f4a-bd0f-1c4b3f7e78c2",
                portID: "output-0",
              },
              target: {
                operatorID: "Aggregate-operator-new-1",
                portID: "input-0",
              },
            },
          ],
          operatorsToDelete: [],
        },
      },
      {
        suggestionID: "suggestion-1",
        suggestion: "Enhance the workflow with sentiment analysis of tweets.",
        suggestionType: "improve",
        changes: {
          operatorsToAdd: [
            {
              operatorType: "HuggingFaceSentimentAnalysis",
              operatorID: "HuggingFaceSentimentAnalysis-5226",
              operatorProperties: {
                attribute: "text",
                "Positive result attribute": "positive_sentiment",
                "Neutral result attribute": "neutral_sentiment",
                "Negative result attribute": "negative_sentiment",
              },
              customDisplayName: "Sentiment Analysis",
            },
          ],
          linksToAdd: [
            {
              linkID: "link-e20f28c0-2da3-406c-be8a-df2a41ff22e3",
              source: {
                operatorID: "PythonUDFV2-operator-3e3c9f53-dae3-4dc4-b724-7ffdb8e7b80c",
                portID: "output-0",
              },
              target: {
                operatorID: "HuggingFaceSentimentAnalysis-5226",
                portID: "input-0",
              },
            },
          ],
          operatorsToDelete: [],
        },
      },
    ],
  };
}
