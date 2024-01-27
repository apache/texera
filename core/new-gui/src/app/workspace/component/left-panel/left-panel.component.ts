import { Component, Type } from "@angular/core";
import { UntilDestroy } from "@ngneat/until-destroy";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { CdkDragDrop, moveItemInArray } from "@angular/cdk/drag-drop";
import { environment } from "../../../../environments/environment";
import { OperatorMenuComponent } from "./operator-menu/operator-menu.component";
import { VersionsListComponent } from "./versions-list/versions-list.component";
import { TimeTravelComponent } from "./time-travel/time-travel.component";

@UntilDestroy()
@Component({
  selector: "texera-left-panel",
  templateUrl: "left-panel.component.html",
  styleUrls: ["left-panel.component.scss"],
})
export class LeftPanelComponent {
  currentComponent: Type<any>;
  title = "Operators";
  screenWidth = window.innerWidth;
  width = 240;
  lastWidth = 0;
  id = -1;
  buttons = [
    { component: OperatorMenuComponent, title: "Operators", icon: "appstore", enabled: true },
    { component: VersionsListComponent, title: "Versions", icon: "schedule", enabled: environment.userSystemEnabled },
    {
      component: TimeTravelComponent,
      title: "Time Travel",
      icon: "clock-circle",
      enabled: environment.userSystemEnabled,
    },
  ];

  constructor() {
    this.currentComponent = OperatorMenuComponent;
  }

  openFrame(title: string, component: Type<any>) {
    if (!this.width) this.width = this.lastWidth;
    this.title = title;
    this.currentComponent = component;
  }
  drop(event: CdkDragDrop<string[]>) {
    moveItemInArray(this.buttons, event.previousIndex, event.currentIndex);
  }

  close() {
    this.currentComponent = null as any;
    this.lastWidth = this.width;
    this.width = 0;
  }
  onResize({ width }: NzResizeEvent) {
    cancelAnimationFrame(this.id);
    this.id = requestAnimationFrame(() => {
      this.width = width!;
    });
  }
}
