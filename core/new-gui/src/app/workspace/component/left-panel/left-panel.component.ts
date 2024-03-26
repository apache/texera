import { Component, HostListener, OnDestroy, OnInit, Type } from "@angular/core";
import { UntilDestroy } from "@ngneat/until-destroy";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { CdkDragDrop, moveItemInArray } from "@angular/cdk/drag-drop";
import { environment } from "../../../../environments/environment";
import { OperatorMenuComponent } from "./operator-menu/operator-menu.component";
import { VersionsListComponent } from "./versions-list/versions-list.component";
import { WorkflowExecutionHistoryComponent } from "../../../dashboard/user/component/user-workflow/ngbd-modal-workflow-executions/workflow-execution-history.component";
import { TimeTravelComponent } from "./time-travel/time-travel.component";

@UntilDestroy()
@Component({
  selector: "texera-left-panel",
  templateUrl: "left-panel.component.html",
  styleUrls: ["left-panel.component.scss"],
})
export class LeftPanelComponent implements OnDestroy, OnInit {
  protected readonly window = window;
  currentComponent: Type<any> | null = null;
  title = "Operators";
  width = 230;
  height = Math.max(300, window.innerHeight * 0.6);
  id = -1;
  currentIndex = 0;
  items = [
    { component: null, title: "", icon: "", enabled: true },
    { component: OperatorMenuComponent, title: "Operators", icon: "appstore", enabled: true },
    { component: VersionsListComponent, title: "Versions", icon: "schedule", enabled: environment.userSystemEnabled },
    {
      component: WorkflowExecutionHistoryComponent,
      title: "Execution History",
      icon: "history",
      enabled: environment.workflowExecutionsTrackingEnabled,
    },
    {
      component: TimeTravelComponent,
      title: "Time Travel",
      icon: "clock-circle",
      enabled: environment.userSystemEnabled && environment.timetravelEnabled,
    },
  ];
  order = [1, 2, 3];

  constructor() {
    this.order = localStorage.getItem("left-panel-order")?.split(",").map(Number) || this.order;
    this.openFrame(Number(localStorage.getItem("left-panel-index") || "1"));
    this.width = Number(localStorage.getItem("left-panel-width")) || this.width;
    this.height = Number(localStorage.getItem("left-panel-height")) || this.height;
  }

  ngOnInit(): void {
    const style = localStorage.getItem("left-panel-style");
    if (style) document.getElementById("left-container")!.style.cssText = style;
  }

  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    localStorage.setItem("left-panel-width", String(this.width));
    localStorage.setItem("left-panel-height", String(this.height));
    localStorage.setItem("left-panel-order", String(this.order));
    localStorage.setItem("left-panel-index", String(this.currentIndex));
    localStorage.setItem("left-panel-style", document.getElementById("left-container")!.style.cssText);
  }

  openFrame(i: number) {
    if (!i) this.width = 0;
    else if (!this.width) this.width = 230;
    this.title = this.items[i].title;
    this.currentComponent = this.items[i].component;
    this.currentIndex = i;
  }
  onDrop(event: CdkDragDrop<string[]>) {
    moveItemInArray(this.order, event.previousIndex, event.currentIndex);
  }
  onResize({ width, height }: NzResizeEvent) {
    cancelAnimationFrame(this.id);
    this.id = requestAnimationFrame(() => {
      this.width = width!;
      this.height = height!;
    });
  }
}
