import { Component, OnInit, OnDestroy, ChangeDetectorRef, TemplateRef, ViewChild, ElementRef } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalService } from "ng-zorro-antd/modal";
import {
  ColumnProfile,
  TableProfile,
} from "../../../../common/type/proto/edu/uci/ics/amber/engine/architecture/worker/tableprofile";
import { WorkflowSuggestionService } from "../../../service/workflow-suggestion/workflow-suggestion.service";
import { ColumnProfileService, SelectedColumnInfo } from "../../../service/column-profile/column-profile.service";
import { finalize } from "rxjs/operators";
import { isDefined } from "../../../../common/util/predicate";
import {
  WorkflowDataCleaningSuggestion,
  WorkflowDataCleaningSuggestionList,
  WorkflowSuggestion,
  WorkflowSuggestionList,
} from "src/app/workspace/types/workflow-suggestion.interface";
import { SchemaAttribute } from "src/app/workspace/types/workflow-compiling.interface";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowCompilingService } from "../../../service/compile-workflow/workflow-compiling.service";
import { SuggestionActionService } from "../../../service/suggestion-action/suggestion-action.service";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { NzMessageService } from "ng-zorro-antd/message";

interface DisplayableDataCleaningSuggestion extends WorkflowDataCleaningSuggestion {
  isExpanded?: boolean;
  details: string; // To store a string representation of changes or other details
  isAccepting?: boolean; // To show loading on the accept button
}

@UntilDestroy()
@Component({
  selector: "texera-column-profile-frame",
  templateUrl: "./column-profile-frame.component.html",
  styleUrls: ["./column-profile-frame.component.scss"],
})
export class ColumnProfileFrameComponent implements OnInit {
  public selectedColumnInfo: SelectedColumnInfo | null = null;
  public columnProfile: ColumnProfile | undefined;
  public tableProfile: TableProfile | undefined;
  public tableSchema: ReadonlyArray<SchemaAttribute> | undefined;

  public columnNumericStatsForTable: Array<{ metric: string; value: string | number | undefined }> = [];
  public barChartData: Array<{ name: string; value: number }> = [];

  // ngx-charts options
  public view: [number, number] = [200, 250]; // Initial value, will be overridden if panel width is available
  public showXAxis = true;
  public showYAxis = true;
  public gradient = false;
  public showLegend = false;
  public showXAxisLabel = true;
  public xAxisLabel = "Category";
  public showYAxisLabel = true;
  public yAxisLabel = "Count";

  // For Data Cleaning Suggestions
  public dataCleaningSuggestions: DisplayableDataCleaningSuggestion[] = [];
  public isLoadingDataCleaningSuggestions: boolean = false;
  private dataCleaningSuggestionsCache: Map<string, DisplayableDataCleaningSuggestion[]> = new Map();
  public lastFetchedSuggestionsForColumn: string | null = null;

  constructor(
    private columnProfileService: ColumnProfileService,
    private workflowSuggestionService: WorkflowSuggestionService,
    private workflowActionService: WorkflowActionService,
    private workflowCompilingService: WorkflowCompilingService,
    private executeWorkflowService: ExecuteWorkflowService,
    private suggestionActionService: SuggestionActionService,
    private messageService: NzMessageService,
    private changeDetectorRef: ChangeDetectorRef,
    private elRef: ElementRef // Inject ElementRef to get host width
  ) {}

  ngOnInit(): void {
    this.columnProfileService
      .getSelectedColumnStream()
      .pipe(untilDestroyed(this))
      .subscribe(selectedInfo => {
        this.selectedColumnInfo = selectedInfo;
        if (selectedInfo) {
          this.columnProfile = selectedInfo.columnProfile;
          this.tableProfile = selectedInfo.tableProfile;
          this.tableSchema = selectedInfo.schema;
          this.loadDisplayData();
        } else {
          this.columnProfile = undefined;
          this.tableProfile = undefined;
          this.tableSchema = undefined;
          this.resetDisplayData();
        }
        this.changeDetectorRef.detectChanges();
      });

    this.updateChartViewWidth();
  }

  // Add AfterViewChecked to update chart width if panel resizes
  // This is a simple way; a more robust solution might use ResizeObserver
  ngAfterViewChecked(): void {
    this.updateChartViewWidth();
  }

  private updateChartViewWidth(): void {
    // Attempt to set chart width based on parent container
    // This requires the .chart-section or its parent to have a defined width
    const hostElement = this.elRef.nativeElement;
    const chartContainer = hostElement.querySelector(".chart-section");
    if (chartContainer) {
      const containerWidth = chartContainer.clientWidth;
      if (containerWidth > 0) {
        // Subtract some padding/margin if chart has internal margins
        const chartWidth = Math.max(150, containerWidth - 20); // Ensure a minimum width
        if (this.view[0] !== chartWidth) {
          this.view = [chartWidth, this.view[1]]; // Keep existing height
          this.changeDetectorRef.detectChanges(); // Trigger change detection if view updated
        }
      }
    } else if (hostElement.clientWidth > 0 && this.view[0] !== hostElement.clientWidth - 20) {
      // Fallback to host element width if .chart-section not found or has no width yet
      const chartWidth = Math.max(150, hostElement.clientWidth - 40); // Wider padding for host
      if (this.view[0] !== chartWidth) {
        this.view = [chartWidth, this.view[1]];
        this.changeDetectorRef.detectChanges();
      }
    }
  }

  private loadDisplayData(): void {
    if (!this.columnProfile) {
      this.resetDisplayData();
      return;
    }
    this.prepareColumnNumericStats(this.columnProfile);
    if (this.columnProfile.categorical && this.columnProfile.statistics) {
      this.prepareBarChartData(this.columnProfile.statistics.categoricalCount);
    } else {
      this.barChartData = [];
    }
    // this.fetchOrGetCachedDataCleaningSuggestions(this.columnProfile);
  }

  private resetDisplayData(): void {
    this.columnNumericStatsForTable = [];
    this.barChartData = [];
    this.dataCleaningSuggestions = [];
    this.isLoadingDataCleaningSuggestions = false;
    this.lastFetchedSuggestionsForColumn = null;
    this.tableSchema = undefined;
  }

  public prepareColumnNumericStats(profile: ColumnProfile | undefined): void {
    this.columnNumericStatsForTable = [];
    if (!profile || !profile.statistics) return;

    const stats = profile.statistics;
    const dataType = profile.dataType.toLowerCase();
    const numericTypes = ["int", "integer", "float", "double", "numeric", "long"];

    this.columnNumericStatsForTable.push({ metric: "Null Count", value: stats.nullCount });
    if (isDefined(stats.uniqueCount)) {
      this.columnNumericStatsForTable.push({
        metric: "Unique Count",
        value: typeof stats.uniqueCount === "number" ? stats.uniqueCount.toLocaleString() : stats.uniqueCount,
      });
    }

    if (this.tableProfile && this.tableProfile.globalProfile) {
      this.columnNumericStatsForTable.push({
        metric: "Total Rows in Table",
        value: this.tableProfile.globalProfile.rowCount.toLocaleString(),
      });
    }

    if (numericTypes.includes(dataType)) {
      if (isDefined(stats.min)) {
        this.columnNumericStatsForTable.push({
          metric: "Min",
          value: typeof stats.min === "number" ? stats.min.toLocaleString() : stats.min,
        });
      }
      if (isDefined(stats.max)) {
        this.columnNumericStatsForTable.push({
          metric: "Max",
          value: typeof stats.max === "number" ? stats.max.toLocaleString() : stats.max,
        });
      }
      if (isDefined(stats.mean) && typeof stats.mean === "number") {
        this.columnNumericStatsForTable.push({ metric: "Mean", value: stats.mean.toFixed(2) });
      } else if (isDefined(stats.mean)) {
        this.columnNumericStatsForTable.push({ metric: "Mean", value: stats.mean });
      }
      if (isDefined(stats.stddev) && typeof stats.stddev === "number") {
        this.columnNumericStatsForTable.push({ metric: "Std Dev", value: stats.stddev.toFixed(2) });
      } else if (isDefined(stats.stddev)) {
        this.columnNumericStatsForTable.push({ metric: "Std Dev", value: stats.stddev });
      }
    }
  }

  public prepareBarChartData(categoricalCount: { [key: string]: number } | undefined): void {
    this.barChartData = [];
    if (!categoricalCount) return;
    this.barChartData = Object.entries(categoricalCount)
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 10);
  }

  private fetchOrGetCachedDataCleaningSuggestions(columnProfile: ColumnProfile): void {
    const cached = this.dataCleaningSuggestionsCache.get(columnProfile.columnName);
    if (cached) {
      this.dataCleaningSuggestions = cached.map(s => ({ ...s, isExpanded: s.isExpanded || false, isAccepting: false }));
      this.lastFetchedSuggestionsForColumn = columnProfile.columnName;
      this.isLoadingDataCleaningSuggestions = false;
    } else {
      this.fetchDataCleaningSuggestions(columnProfile);
    }
  }

  public fetchDataCleaningSuggestions(columnProfile: ColumnProfile | undefined) {
    if (!columnProfile || !this.tableProfile || !this.selectedColumnInfo?.operatorId || !this.tableSchema) {
      this.dataCleaningSuggestions = [];
      this.isLoadingDataCleaningSuggestions = false;
      return;
    }

    this.isLoadingDataCleaningSuggestions = true;
    this.dataCleaningSuggestions = [];
    this.workflowSuggestionService
      .getDataCleaningSuggestions(
        this.selectedColumnInfo.operatorId,
        this.tableSchema,
        this.tableProfile,
        columnProfile.columnName
      )
      .pipe(
        finalize(() => {
          this.isLoadingDataCleaningSuggestions = false;
          this.changeDetectorRef.detectChanges();
        }),
        untilDestroyed(this)
      )
      .subscribe(
        (response: WorkflowDataCleaningSuggestionList) => {
          this.dataCleaningSuggestions = response.suggestions.map(s => ({
            ...s,
            isExpanded: false,
            details: s.details,
            isAccepting: false,
          }));
          this.dataCleaningSuggestionsCache.set(columnProfile.columnName, this.dataCleaningSuggestions);
          this.lastFetchedSuggestionsForColumn = columnProfile.columnName;
        },
        (error: unknown) => {
          console.error("Error fetching data cleaning suggestions:", error);
          this.dataCleaningSuggestions = [];
        }
      );
  }

  public refreshDataCleaningSuggestions(): void {
    if (this.columnProfile) {
      this.dataCleaningSuggestionsCache.delete(this.columnProfile.columnName);
      this.fetchDataCleaningSuggestions(this.columnProfile);
    }
  }

  public toggleSuggestionExpansion(suggestion: DisplayableDataCleaningSuggestion): void {
    suggestion.isExpanded = !suggestion.isExpanded;
    this.changeDetectorRef.detectChanges();
  }

  public acceptSuggestion(suggestion: DisplayableDataCleaningSuggestion, event: MouseEvent): void {
    event.stopPropagation();
    if (!this.selectedColumnInfo || !this.selectedColumnInfo.tableProfile || suggestion.isAccepting) {
      if (!this.selectedColumnInfo) console.warn("acceptSuggestion: selectedColumnInfo is null.");
      else if (!this.selectedColumnInfo.schema) console.warn("acceptSuggestion: selectedColumnInfo.schema is null.");
      else if (suggestion.isAccepting) console.warn("acceptSuggestion: suggestion is already being accepted.");
      return;
    }

    suggestion.isAccepting = true;
    this.messageService.loading(`Attempting to apply action for: "${suggestion.suggestion}"...`, { nzDuration: 0 });

    const operatorIDToSchema: { [key: string]: ReadonlyArray<SchemaAttribute> } = {};
    operatorIDToSchema[this.selectedColumnInfo.operatorId] = this.selectedColumnInfo.schema;

    this.workflowSuggestionService
      .getSuggestions(
        this.workflowActionService.getWorkflow(),
        this.workflowCompilingService.getWorkflowCompilationStateInfo(),
        this.executeWorkflowService.getExecutionState(),
        suggestion.suggestion,
        [this.selectedColumnInfo.operatorId],
        operatorIDToSchema
      )
      .pipe(
        finalize(() => {
          suggestion.isAccepting = false;
          this.messageService.remove();
          this.changeDetectorRef.detectChanges();
        }),
        untilDestroyed(this)
      )
      .subscribe(
        (actionableSuggestions: WorkflowSuggestionList) => {
          if (actionableSuggestions.suggestions && actionableSuggestions.suggestions.length > 0) {
            const firstActionableSuggestion = actionableSuggestions.suggestions[0];
            this.messageService.success(
              `Applying action: "${firstActionableSuggestion.suggestion}" based on your request.`
            );
            this.suggestionActionService.applySuggestion(firstActionableSuggestion);
            this.removeSuggestionFromList(suggestion);
          } else {
            this.messageService.warning(
              `Could not find an actionable Texera suggestion for: "${suggestion.suggestion}"`
            );
          }
        },
        (error: unknown) => {
          console.error("Error getting actionable suggestions:", error);
          this.messageService.error("Failed to get actionable suggestions.");
        }
      );
  }

  public rejectSuggestion(suggestion: DisplayableDataCleaningSuggestion, event: MouseEvent): void {
    event.stopPropagation();
    this.removeSuggestionFromList(suggestion);
  }

  private removeSuggestionFromList(suggestionToRemove: DisplayableDataCleaningSuggestion): void {
    this.dataCleaningSuggestions = this.dataCleaningSuggestions.filter(
      s => s.suggestionID !== suggestionToRemove.suggestionID
    );
    if (this.columnProfile) {
      const cached = this.dataCleaningSuggestionsCache.get(this.columnProfile.columnName);
      if (cached) {
        this.dataCleaningSuggestionsCache.set(
          this.columnProfile.columnName,
          cached.filter(s => s.suggestionID !== suggestionToRemove.suggestionID)
        );
      }
    }
    this.changeDetectorRef.detectChanges();
  }

  public onChartSelect(event: any): void {
    console.log("Chart event:", event);
  }
}
