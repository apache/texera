import {Component, OnInit} from "@angular/core";

import {UntilDestroy, untilDestroyed} from "@ngneat/until-destroy";
import {DynamicComponentConfig} from "../../../common/type/dynamic-component-config";
import {OperatorMenuFrameComponent} from "./operator-menu-frame/operator-menu-frame.component";
import {VersionsListFrameComponent} from "./versions-display/versions-display.component";
import {merge} from "rxjs";
import {
  DISPLAY_WORKFLOW_VERIONS_EVENT,
  WorkflowVersionService
} from "../../../dashboard/service/workflow-version/workflow-version.service";

export type LeftFrameComponent =
  | OperatorMenuFrameComponent
  | VersionsListFrameComponent;

export type LeftFrameComponentConfig = DynamicComponentConfig<LeftFrameComponent>;


@UntilDestroy()
@Component({
  selector: "texera-left-panel",
  templateUrl: "./left-panel.component.html",
  styleUrls: ["./left-panel.component.scss"],
})
export class LeftPanelComponent implements OnInit {
  frameComponentConfig?: LeftFrameComponentConfig;

  currentOperatorId?: string;

  constructor(
    private workflowVersionService: WorkflowVersionService
  ) {
  }

  ngOnInit(): void {
    this.registerHighlightEventsHandler();
    this.switchFrameComponent({
      component: OperatorMenuFrameComponent,
      componentInputs: {},
    });
  }

  switchFrameComponent(targetConfig?: LeftFrameComponentConfig) {
    if (
      this.frameComponentConfig?.component === targetConfig?.component &&
      this.frameComponentConfig?.componentInputs === targetConfig?.componentInputs
    ) {
      return;
    }

    this.frameComponentConfig = targetConfig;
  }

  registerHighlightEventsHandler() {
    merge(
      this.workflowVersionService.workflowVersionsDisplayObservable()
    )
      .pipe(untilDestroyed(this))
      .subscribe(event => {
          const isDisplayWorkflowVersions = event.length === 1 && event[0] === DISPLAY_WORKFLOW_VERIONS_EVENT;
          if (isDisplayWorkflowVersions) {
            this.switchFrameComponent({
              component: VersionsListFrameComponent,
            });
          } else {
            this.switchFrameComponent({
              component: OperatorMenuFrameComponent
            });
          }
        }
      )
    ;
  }
}
