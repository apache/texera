import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { OperatorMenuComponent } from "./operator-menu/operator-menu.component";
import { VersionsListComponent } from "./versions-list/versions-list.component";
import { ComponentType } from "@angular/cdk/overlay";
import {
  OPEN_VERSIONS_FRAME_EVENT,
  WorkflowVersionService,
} from "../../../dashboard/user/service/workflow-version/workflow-version.service";
import { NzResizeEvent } from "ng-zorro-antd/resizable";

@UntilDestroy()
@Component({
  selector: "texera-left-panel",
  templateUrl: "left-panel.component.html",
  styleUrls: ["left-panel.component.scss"],
})
export class LeftPanelComponent implements OnInit {
  currentComponent: ComponentType<OperatorMenuComponent | VersionsListComponent>;
  screenWidth = window.innerWidth;
  width = 200;
  id = -1;
  disabled = false;

  onResize({ width }: NzResizeEvent): void {
    cancelAnimationFrame(this.id);
    this.id = requestAnimationFrame(() => {
      this.width = width!;
    });
  }

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
          (this.currentComponent = event === OPEN_VERSIONS_FRAME_EVENT ? VersionsListComponent : OperatorMenuComponent)
      );
  }
}
