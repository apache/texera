import { Component, OnInit, Type } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { OperatorMenuFrameComponent } from "./operator-menu-frame/operator-menu-frame.component";
import { VersionsFrameComponent } from "./versions-frame/versions-frame.component";
import {
  OPEN_VERSIONS_FRAME_EVENT,
  WorkflowVersionService,
} from "../../../dashboard/user/service/workflow-version/workflow-version.service";

@UntilDestroy()
@Component({
  selector: "texera-left-panel",
  templateUrl: "left-panel.component.html",
  styleUrls: ["left-panel.component.scss"],
})
export class LeftPanelComponent implements OnInit {
  currentComponent: Type<any>;

  constructor(private workflowVersionService: WorkflowVersionService) {
    this.currentComponent = OperatorMenuFrameComponent;
  }

  ngOnInit(): void {
    this.registerVersionDisplayEventsHandler();
  }

  registerVersionDisplayEventsHandler(): void {
    this.workflowVersionService
      .workflowVersionsDisplayObservable()
      .pipe(untilDestroyed(this))
      .subscribe(
        event =>
          (this.currentComponent =
            event === OPEN_VERSIONS_FRAME_EVENT ? VersionsFrameComponent : OperatorMenuFrameComponent)
      );
  }
}
