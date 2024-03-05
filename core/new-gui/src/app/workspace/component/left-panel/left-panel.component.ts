import { Component, HostListener, OnDestroy, OnInit, Type } from "@angular/core";
import { UntilDestroy } from "@ngneat/until-destroy";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { CdkDragDrop, moveItemInArray } from "@angular/cdk/drag-drop";
import { environment } from "../../../../environments/environment";
import { OperatorMenuComponent } from "./operator-menu/operator-menu.component";
import { VersionsListComponent } from "./versions-list/versions-list.component";
import { TimeTravelComponent } from "./time-travel/time-travel.component";
import { EnvironmentComponent } from "./environment/environment.component";

@UntilDestroy()
@Component({
  selector: "texera-left-panel",
  templateUrl: "left-panel.component.html",
  styleUrls: ["left-panel.component.scss"],
})
export class LeftPanelComponent implements OnDestroy, OnInit {
  currentComponent: Type<any> = null as any;
  title = "Operators";
  screenWidth = window.innerWidth;
  width = 240;
  id = -1;
  currentIndex = 0;
  items = [
    { component: null as any, title: "", icon: "", enabled: true },
    { component: OperatorMenuComponent, title: "Operators", icon: "appstore", enabled: true },
    { component: VersionsListComponent, title: "Versions", icon: "schedule", enabled: environment.userSystemEnabled },
    {
      component: TimeTravelComponent,
      title: "Time Travel",
      icon: "clock-circle",
      enabled: environment.userSystemEnabled,
    },
    {
      component: EnvironmentComponent,
      title: "Environment",
      icon: "dashboard",
      enabled: environment.userSystemEnabled,
    },
  ];
  order = [1, 2, 3, 4];

  constructor() {
    const order = localStorage.getItem("left-panel-order");
    if (order) this.order = order.split(",").map(Number);
    this.openFrame(Number(localStorage.getItem("left-panel-index") || "1"));
    const width = localStorage.getItem("left-panel-width");
    if (width) this.width = Number(width);
  }

  ngOnInit(): void {
    const style = localStorage.getItem("left-panel-style");
    if (style) document.getElementById("left-panel-container")!.style.cssText = style;
  }

  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    localStorage.setItem("left-panel-width", String(this.width));
    localStorage.setItem("left-panel-order", String(this.order));
    localStorage.setItem("left-panel-index", String(this.currentIndex));
    localStorage.setItem("left-panel-style", document.getElementById("left-panel-container")!.style.cssText);
  }

  openFrame(i: number) {
    if (!this.width) this.width = 240;
    this.title = this.items[i].title;
    this.currentComponent = this.items[i].component;
    this.currentIndex = i;
  }
  onDrop(event: CdkDragDrop<string[]>) {
    moveItemInArray(this.order, event.previousIndex, event.currentIndex);
  }

  onClose() {
    this.currentComponent = null as any;
    this.width = 0;
    this.currentIndex = 0;
  }
  onResize({ width }: NzResizeEvent) {
    cancelAnimationFrame(this.id);
    this.id = requestAnimationFrame(() => {
      this.width = width!;
    });
  }
}
