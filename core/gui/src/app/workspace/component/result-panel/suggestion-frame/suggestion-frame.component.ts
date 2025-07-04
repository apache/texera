import { Component, OnInit, OnDestroy, ChangeDetectorRef, NgZone } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowSuggestionService } from "../../../service/workflow-suggestion/workflow-suggestion.service";
import { NzMessageService } from "ng-zorro-antd/message";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WorkflowCompilingService } from "../../../service/compile-workflow/workflow-compiling.service";
import { Subscription, interval, Observable } from "rxjs";
import { WorkflowSuggestion, WorkflowSuggestionList } from "../../../types/workflow-suggestion.interface";
import { SuggestionActionService } from "../../../service/suggestion-action/suggestion-action.service";

/**
 * SuggestionFrameComponent is a wrapper for the workflow suggestion functionality
 * that allows it to be displayed in the result panel as a tab.
 */
@UntilDestroy()
@Component({
  selector: "texera-suggestion-frame",
  templateUrl: "./suggestion-frame.component.html",
  styleUrls: ["./suggestion-frame.component.scss"],
})
export class SuggestionFrameComponent implements OnInit, OnDestroy {
  public suggestions: WorkflowSuggestion[] = [];
  public canModify = true;
  public loadingSuggestions = false;
  public intentionText = "";

  // Observables from SuggestionActionService for template binding
  public activePreviewId: string | null = null;
  public isInPreviewMode: boolean = false;

  private tabFocusInterval: Subscription | null = null;
  private boundHandleDocumentClick: any;

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowCompilingService: WorkflowCompilingService,
    private workflowExecuteService: ExecuteWorkflowService,
    private messageService: NzMessageService,
    private workflowSuggestionService: WorkflowSuggestionService,
    private suggestionActionService: SuggestionActionService,
    private cdr: ChangeDetectorRef,
    private ngZone: NgZone
  ) {
    this.suggestionActionService.activePreviewId$.pipe(untilDestroyed(this)).subscribe(id => {
      this.activePreviewId = id;
      this.cdr.detectChanges();
    });

    this.suggestionActionService.isInPreviewMode$.pipe(untilDestroyed(this)).subscribe(inPreview => {
      this.isInPreviewMode = inPreview;
      if (inPreview) {
        this.addDocumentClickListener();
      } else {
        this.removeDocumentClickListener();
      }
      this.cdr.detectChanges();
    });
    this.boundHandleDocumentClick = this.handleDocumentClick.bind(this);
  }

  ngOnInit(): void {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(canModify => (this.canModify = canModify));

    this.workflowSuggestionService
      .getSuggestionsListStream()
      .pipe(untilDestroyed(this))
      .subscribe(list => {
        this.suggestions = list.suggestions;
        this.cdr.detectChanges();
      });

    this.workflowSuggestionService
      .getLoadingStream()
      .pipe(untilDestroyed(this))
      .subscribe(isLoading => {
        this.loadingSuggestions = isLoading;
        this.cdr.detectChanges();
      });
  }

  ngOnDestroy(): void {
    this.removeDocumentClickListener();

    if (this.tabFocusInterval) {
      this.tabFocusInterval.unsubscribe();
      this.tabFocusInterval = null;
    }
  }

  private addDocumentClickListener(): void {
    if (!document.hasOwnProperty("suggestionsClickListener")) {
      document.addEventListener("click", this.boundHandleDocumentClick, true);
      // @ts-ignore
      document.suggestionsClickListener = true;
    }
  }

  private removeDocumentClickListener(): void {
    document.removeEventListener("click", this.boundHandleDocumentClick, true);
    // @ts-ignore
    delete document.suggestionsClickListener;
  }

  private handleDocumentClick(event: MouseEvent): void {
    if (this.isInPreviewMode) {
      const actionButton = (event.target as HTMLElement).closest(".suggestion-actions button");
      const target = event.target as HTMLElement;

      if (actionButton) return;

      if (target && (target.closest(".ant-tabs-tab") || target.closest("a[href]"))) {
        if (!target.closest("texera-suggestion-frame")) {
          event.preventDefault();
          event.stopPropagation();
          this.messageService.warning("Please cancel the preview first before changing tabs");
          this.focusSuggestionTab();
        }
      }
    }
  }

  public refreshSuggestions(): void {
    if (this.isInPreviewMode) {
      this.messageService.info("Please cancel the active preview before refreshing suggestions.");
      return;
    }
    const operators = this.workflowActionService.getTexeraGraph().getAllOperators();
    if (operators.length === 0) {
      this.messageService.info("Cannot generate suggestions for an empty workflow.");
      return;
    }
    const focusedOperatorIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    const intention = this.intentionText.trim();

    this.workflowSuggestionService
      .getSuggestions(
        this.workflowActionService.getWorkflow(),
        this.workflowCompilingService.getWorkflowCompilationStateInfo(),
        this.workflowExecuteService.getExecutionState(),
        this.intentionText.trim(),
        this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs(),
        {}
      )
      .pipe(untilDestroyed(this))
      .subscribe();
  }

  public togglePreview(suggestion: WorkflowSuggestion): void {
    this.suggestionActionService.togglePreview(suggestion);
    if (this.isInPreviewMode) {
      this.focusSuggestionTab();
    }
  }

  private focusSuggestionTab(): void {
    setTimeout(() => {
      const suggestionTab = document.querySelector(".ant-tabs-tab[aria-controls*=\"Suggestions\"]") as HTMLElement;
      if (suggestionTab) {
        suggestionTab.click();
        if (this.isInPreviewMode) {
          if (this.tabFocusInterval) this.tabFocusInterval.unsubscribe();
          this.tabFocusInterval = interval(300)
            .pipe(untilDestroyed(this))
            .subscribe(() => {
              if (this.isInPreviewMode) {
                const activeTab = document.querySelector(".ant-tabs-tab-active");
                const isSuggestionTabActive =
                  activeTab &&
                  (activeTab.textContent?.includes("Suggestions") ||
                    activeTab.getAttribute("aria-controls")?.includes("Suggestions"));
                if (!isSuggestionTabActive) {
                  const currentSuggestionTab = document.querySelector(
                    ".ant-tabs-tab[aria-controls*=\"Suggestions\"]"
                  ) as HTMLElement;
                  if (currentSuggestionTab) currentSuggestionTab.click();
                }
              } else if (this.tabFocusInterval) {
                this.tabFocusInterval.unsubscribe();
                this.tabFocusInterval = null;
              }
            });
        }
      }
    }, 50);
  }

  public dislikeSuggestion(suggestion: WorkflowSuggestion): void {
    if (this.activePreviewId === suggestion.suggestionID) {
      this.suggestionActionService.clearPreviewAndRestoreWorkflow();
    }
    this.workflowSuggestionService.removeSuggestionById(suggestion.suggestionID);
    this.messageService.info("Suggestion removed from the list.");
  }

  public cancelPreview(): void {
    this.suggestionActionService.clearPreviewAndRestoreWorkflow();
  }

  public applySuggestion(suggestion: WorkflowSuggestion): void {
    this.suggestionActionService.applySuggestion(suggestion);
  }
}
