import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { OperatorMenuComponent } from "./operator-menu/operator-menu.component";
import { VersionsFrameComponent } from "./versions-frame/versions-frame.component";
import { ComponentType } from "@angular/cdk/overlay";
import {
  OPEN_VERSIONS_FRAME_EVENT,
  WorkflowVersionService,
} from "../../../dashboard/user/service/workflow-version/workflow-version.service";
import { Version } from "../../../../environments/version";

@UntilDestroy()
@Component({
  selector: "texera-left-panel",
  templateUrl: "left-panel.component.html",
  styleUrls: ["left-panel.component.scss"],
})
export class LeftPanelComponent implements OnInit {
  currentComponent: ComponentType<any>;
  public gitCommitHash: string = Version.raw;

  constructor(private workflowVersionService: WorkflowVersionService) {
    this.currentComponent = OperatorMenuComponent;
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
          (this.currentComponent = event === OPEN_VERSIONS_FRAME_EVENT ? VersionsFrameComponent : OperatorMenuComponent)
      );
  }
}
