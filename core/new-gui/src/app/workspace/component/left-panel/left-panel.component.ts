import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DynamicComponentConfig } from "../../../common/type/dynamic-component-config";
import { OperatorMenuFrameComponent } from "./operator-menu-frame/operator-menu-frame.component";
import { VersionsFrameComponent } from "./versions-frame/versions-frame.component";
import {
  OPEN_VERSIONS_FRAME_EVENT,
  WorkflowVersionService,
} from "../../../dashboard/user/service/workflow-version/workflow-version.service";

export type LeftFrameComponent = OperatorMenuFrameComponent | VersionsFrameComponent;
export type LeftFrameComponentConfig = DynamicComponentConfig<LeftFrameComponent>;

@UntilDestroy()
@Component({
  selector: "texera-left-panel",
  templateUrl: "./left-panel.component.html",
  styleUrls: ["./left-panel.component.scss"],
})
export class LeftPanelComponent implements OnInit {
  frameComponentConfig?: LeftFrameComponentConfig;

  constructor(private workflowVersionService: WorkflowVersionService) {}

  ngOnInit(): void {
    this.registerVersionDisplayEventsHandler();
    this.switchFrameComponent({
      component: OperatorMenuFrameComponent,
      componentInputs: {},
    });
  }

  switchFrameComponent(targetConfig?: LeftFrameComponentConfig): void {
    if (
      this.frameComponentConfig?.component === targetConfig?.component &&
      this.frameComponentConfig?.componentInputs === targetConfig?.componentInputs
    ) {
      return;
    }

    this.frameComponentConfig = targetConfig;
  }

  registerVersionDisplayEventsHandler(): void {
    this.workflowVersionService
      .workflowVersionsDisplayObservable()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        if (event === OPEN_VERSIONS_FRAME_EVENT) {
          this.switchFrameComponent({
            component: VersionsFrameComponent,
          });
        } else {
          // CLOSE_VERSIONS_FRAME_EVENT
          this.switchFrameComponent({
            component: OperatorMenuFrameComponent,
          });
        }
      });
  }
}
