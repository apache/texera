import { Component, HostListener, OnDestroy, OnInit, Type, ViewChild, ElementRef } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { CdkDragDrop, moveItemInArray } from "@angular/cdk/drag-drop";
import { environment } from "../../../../environments/environment";
import { OperatorMenuComponent } from "./operator-menu/operator-menu.component";
import { VersionsListComponent } from "./versions-list/versions-list.component";
import { WorkflowExecutionHistoryComponent } from "../../../dashboard/component/user/user-workflow/ngbd-modal-workflow-executions/workflow-execution-history.component";
import { TimeTravelComponent } from "./time-travel/time-travel.component";
import { SettingsComponent } from "./settings/settings.component";
import { calculateTotalTranslate3d } from "../../../common/util/panel-dock";
import { PanelService } from "../../service/panel/panel.service";
@UntilDestroy()
@Component({
  selector: "texera-left-panel",
  templateUrl: "left-panel.component.html",
  styleUrls: ["left-panel.component.scss"],
})
export class LeftPanelComponent implements OnDestroy, OnInit {
  @ViewChild("leftContainer") leftContainer!: ElementRef;
  protected readonly window = window;
  currentComponent: Type<any> | null = null;
  title = "Operators";
  width = 300;
  height = Math.max(400, window.innerHeight * 0.6);
  id = -1;
  currentIndex = 0;
  items = [
    { component: null, title: "", icon: "", enabled: true },
    { component: OperatorMenuComponent, title: "Operators", icon: "appstore", enabled: true },
    { component: VersionsListComponent, title: "Versions", icon: "schedule", enabled: environment.userSystemEnabled },
    {
      component: SettingsComponent,
      title: "Settings",
      icon: "setting",
      enabled: true,
    },
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

  order = Array.from({ length: this.items.length - 1 }, (_, index) => index + 1);
  dragPosition = { x: 0, y: 0 };
  returnPosition = { x: 0, y: 0 };
  isDocked = true;
  hasManuallyResized = false;

  constructor(private panelService: PanelService) {
    const savedOrder = localStorage.getItem("left-panel-order")?.split(",").map(Number);
    this.order = savedOrder && new Set(savedOrder).size === new Set(this.order).size ? savedOrder : this.order;

    const savedIndex = Number(localStorage.getItem("left-panel-index"));
    this.openFrame(savedIndex < this.items.length && this.items[savedIndex].enabled ? savedIndex : 1);

    this.width = Number(localStorage.getItem("left-panel-width")) || this.width;
    this.height = Number(localStorage.getItem("left-panel-height")) || this.height;
  }

  ngOnInit(): void {
    const style = localStorage.getItem("left-panel-style");
    if (style) document.getElementById("left-container")!.style.cssText = style;
    const translates = document.getElementById("left-container")!.style.transform;
    const [xOffset, yOffset, _] = calculateTotalTranslate3d(translates);
    this.returnPosition = { x: -xOffset, y: -yOffset };
    this.isDocked = this.dragPosition.x === this.returnPosition.x && this.dragPosition.y === this.returnPosition.y;
    this.panelService.closePanelStream.pipe(untilDestroyed(this)).subscribe(() => this.openFrame(0));
    this.panelService.resetPanelStream.pipe(untilDestroyed(this)).subscribe(() => {
      this.resetPanelPosition();
      this.openFrame(1);
    });
  }

  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    localStorage.setItem("left-panel-width", String(this.width));
    localStorage.setItem("left-panel-height", String(this.height));
    localStorage.setItem("left-panel-order", String(this.order));
    localStorage.setItem("left-panel-index", String(this.currentIndex));

    const leftContainer = document.getElementById("left-container");
    if (leftContainer) {
      localStorage.setItem("left-panel-style", leftContainer.style.cssText);
    }
  }

  openFrame(i: number) {
    if (!i) {
      this.width = 0;
      this.height = 65;
    } else if (!this.width) {
      this.width = 230;
      this.height = 400;
    }
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
      this.hasManuallyResized = true;
    });
  }

  resizeToFitContent() {
    setTimeout(() => {
      if (this.hasManuallyResized) {
        this.hasManuallyResized = false;
        return;
      }

      // Clone the element to measure it without affecting the layout
      const element = this.leftContainer.nativeElement as HTMLElement;
      const clone = element.cloneNode(true) as HTMLElement;
      clone.style.position = "absolute";
      clone.style.visibility = "hidden";
      clone.style.height = "auto";
      clone.style.width = `${element.offsetWidth}px`;
      clone.style.top = "0"; // Reset the top position
      clone.style.left = "0"; // Reset the left position
      document.body.appendChild(clone);
      const naturalHeight = clone.offsetHeight;
      document.body.removeChild(clone);

      element.style.transition = "height 0.3s ease";
      const maxHeight = window.innerHeight * 0.85;
      if (this.height < naturalHeight) {
        this.height = Math.min(naturalHeight, maxHeight);
      }
      setTimeout(() => {
        element.style.transition = "none"; // Disable the transition after the animation completes
      }, 300);
    }, 175);
  }

  resetPanelPosition() {
    this.dragPosition = { x: this.returnPosition.x, y: this.returnPosition.y };
    this.isDocked = true;
  }

  handleDragStart() {
    this.isDocked = false;
  }
}
