import { Component } from "@angular/core";
import { UntilDestroy } from "@ngneat/until-destroy";
import { OperatorMenuComponent } from "./operator-menu/operator-menu.component";
import { VersionsListComponent } from "./versions-list/versions-list.component";
import { ComponentType } from "@angular/cdk/overlay";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { TimeTravelComponent } from "./time-travel/time-travel.component";
import { environment } from "../../../../environments/environment";

@UntilDestroy()
@Component({
  selector: "texera-left-panel",
  templateUrl: "left-panel.component.html",
  styleUrls: ["left-panel.component.scss"],
})
export class LeftPanelComponent {
  currentComponent: ComponentType<OperatorMenuComponent | VersionsListComponent | TimeTravelComponent>;
  title = "Operators";
  screenWidth = window.innerWidth;
  width = 240;
  id = -1;
  disabled = false;

  // whether user dashboard is enabled and accessible from the workspace
  public userSystemEnabled: boolean = environment.userSystemEnabled;

  onResize({ width }: NzResizeEvent): void {
    cancelAnimationFrame(this.id);
    this.id = requestAnimationFrame(() => {
      this.width = width!;
    });
  }

  constructor() {
    this.currentComponent = OperatorMenuComponent;
  }

  openVersionsFrame(): void {
    this.currentComponent = VersionsListComponent;
    this.title = "Versions";
  }

  openTimeTravelFrame(): void {
    this.currentComponent = TimeTravelComponent;
    this.title = "Time Travel";
  }

  openOperatorMenu(): void {
    this.currentComponent = OperatorMenuComponent;
    this.title = "Operators";
  }
}
