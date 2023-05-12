import { ChangeDetectorRef, Component, OnInit } from "@angular/core";
import { merge } from "rxjs";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OperatorPropertyEditFrameComponent } from "./operator-property-edit-frame/operator-property-edit-frame.component";
import { BreakpointPropertyEditFrameComponent } from "./breakpoint-property-edit-frame/breakpoint-property-edit-frame.component";
import { DynamicComponentConfig } from "../../../common/type/dynamic-component-config";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import {
  DISPLAY_WORKFLOW_VERIONS_EVENT,
  WorkflowVersionService,
} from "src/app/dashboard/service/workflow-version/workflow-version.service";
import { VersionsListDisplayComponent } from "./versions-display/versions-display.component";
import { auditTime, filter } from "rxjs/operators";

export type PropertyEditFrameComponent =
  | OperatorPropertyEditFrameComponent
  | BreakpointPropertyEditFrameComponent
  | VersionsListDisplayComponent;

export type PropertyEditFrameConfig = DynamicComponentConfig<PropertyEditFrameComponent>;

/**
 * PropertyEditorComponent is the panel that allows user to edit operator properties.
 * Depending on the highlighted operator or link, it displays OperatorPropertyEditFrameComponent
 * or BreakpointPropertyEditFrameComponent accordingly
 *
 */
@UntilDestroy()
@Component({
  selector: "texera-property-editor",
  templateUrl: "./property-editor.component.html",
  styleUrls: ["./property-editor.component.scss"],
})
export class PropertyEditorComponent implements OnInit {
  frameComponentConfig?: PropertyEditFrameConfig;

  constructor(
    public workflowActionService: WorkflowActionService,
    public workflowVersionService: WorkflowVersionService,
    private changeDetectorRef: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.registerHighlightEventsHandler();
  }

  switchFrameComponent(targetConfig?: PropertyEditFrameConfig) {
    if (
      this.frameComponentConfig?.component === targetConfig?.component &&
      this.frameComponentConfig?.componentInputs === targetConfig?.componentInputs
    ) {
      return;
    }

    this.frameComponentConfig = targetConfig;
  }

  /**
   * This method changes the property editor according to how operators are highlighted on the workflow editor.
   *
   * Displays the form of the highlighted operator if only one operator is highlighted;
   * Displays the form of the link breakpoint if only one link is highlighted;
   * hides the form if no operator/link is highlighted or multiple operators and/or groups and/or links are highlighted.
   */
  registerHighlightEventsHandler() {
    merge(
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getLinkHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getLinkUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointCommentBoxHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointCommentBoxUnhighlightStream(),
      this.workflowVersionService.workflowVersionsDisplayObservable()
    )
      .pipe(
        filter(() => this.workflowActionService.getTexeraGraph().getSyncTexeraGraph()),
        untilDestroyed(this)
      )
      .subscribe(event => {
        const isDisplayWorkflowVersions = event.length === 1 && event[0] === DISPLAY_WORKFLOW_VERIONS_EVENT;

        const highlightedOperators = this.workflowActionService
          .getJointGraphWrapper()
          .getCurrentHighlightedOperatorIDs();
        const highlightedGroups = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();
        const highlightLinks = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs();
        const highlightCommentBoxes = this.workflowActionService
          .getJointGraphWrapper()
          .getCurrentHighlightedCommentBoxIDs();

        if (isDisplayWorkflowVersions) {
          this.switchFrameComponent({
            component: VersionsListDisplayComponent,
          });
        } else if (highlightedOperators.length === 1 && highlightedGroups.length === 0 && highlightLinks.length === 0) {
          this.switchFrameComponent({
            component: OperatorPropertyEditFrameComponent,
            componentInputs: { currentOperatorId: highlightedOperators[0] },
          });
        } else if (highlightLinks.length === 1 && highlightedGroups.length === 0 && highlightedOperators.length === 0) {
          this.switchFrameComponent({
            component: BreakpointPropertyEditFrameComponent,
            componentInputs: { currentLinkId: highlightLinks[0] },
          });
        } else {
          this.switchFrameComponent(undefined);
          this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("currentlyEditing", undefined);
        }
        this.changeDetectorRef.detectChanges();
      });
  }
}
