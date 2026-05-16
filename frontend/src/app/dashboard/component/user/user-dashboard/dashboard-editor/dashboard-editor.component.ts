/**
 * Dashboard editor. Renders the widget grid with edit / view mode toggle,
 * widget resize handles, drag-to-move (via mouse), and an "Add Widget" modal.
 */

import { Component, ElementRef, HostListener, OnDestroy, OnInit, ViewChild } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { ActivatedRoute, Router } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalService } from "ng-zorro-antd/modal";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzPopconfirmDirective } from "ng-zorro-antd/popconfirm";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { DashboardService } from "../dashboard.service";
import { Dashboard, DashboardWidget, WidgetLayout } from "../dashboard.types";
import { DashboardWidgetComponent } from "../widgets/dashboard-widget.component";
import { AddWidgetModalComponent, AddWidgetModalData } from "../add-widget-modal/add-widget-modal.component";
import { DASHBOARD_USER_DASHBOARD } from "../../../../../app-routing.constant";

const MIN_WIDTH = 160;
const MIN_HEIGHT = 120;

type DragMode =
  | {
      kind: "move";
      widgetId: string;
      startMouseX: number;
      startMouseY: number;
      startWidgetX: number;
      startWidgetY: number;
      moved: boolean;
    }
  | {
      kind: "resize";
      widgetId: string;
      startMouseX: number;
      startMouseY: number;
      startWidth: number;
      startHeight: number;
    };

@UntilDestroy()
@Component({
  selector: "texera-dashboard-editor",
  templateUrl: "./dashboard-editor.component.html",
  styleUrls: ["./dashboard-editor.component.scss"],
  imports: [
    CommonModule,
    FormsModule,
    DashboardWidgetComponent,
    NzButtonComponent,
    NzIconDirective,
    NzInputDirective,
    NzPopconfirmDirective,
    NzTooltipDirective,
  ],
})
export class DashboardEditorComponent implements OnInit, OnDestroy {
  @ViewChild("grid", { static: false }) gridEl?: ElementRef<HTMLDivElement>;

  dashboard?: Dashboard;
  mode: "edit" | "view" = "view";
  editingName = false;
  nameDraft = "";

  private drag: DragMode | null = null;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private dashboardService: DashboardService,
    private modal: NzModalService
  ) {}

  ngOnInit(): void {
    this.route.paramMap.pipe(untilDestroyed(this)).subscribe(p => {
      const id = p.get("id");
      if (id) this.loadDashboard(id);
    });
  }

  ngOnDestroy(): void {
    this.drag = null;
  }

  private loadDashboard(id: string): void {
    this.dashboardService
      .list()
      .pipe(untilDestroyed(this))
      .subscribe(list => {
        this.dashboard = list.find(d => d.id === id);
        if (this.dashboard) {
          this.nameDraft = this.dashboard.name;
        }
      });
  }

  backToList(): void {
    this.router.navigate([DASHBOARD_USER_DASHBOARD]);
  }

  toggleMode(): void {
    this.mode = this.mode === "edit" ? "view" : "edit";
    this.editingName = false;
  }

  startEditName(): void {
    if (!this.dashboard) return;
    this.editingName = true;
    this.nameDraft = this.dashboard.name;
  }

  saveName(): void {
    if (!this.dashboard || !this.nameDraft.trim()) {
      this.editingName = false;
      return;
    }
    this.dashboardService.rename(this.dashboard.id, this.nameDraft);
    this.editingName = false;
  }

  openAddWidget(): void {
    if (!this.dashboard) return;
    const data: AddWidgetModalData = {
      onAdd: (widget, source) => {
        if (!this.dashboard) return;
        this.dashboardService.addWidget(this.dashboard.id, widget, source);
      },
    };
    this.modal.create({
      nzContent: AddWidgetModalComponent,
      nzData: data,
      nzFooter: null,
      nzWidth: 760,
      nzCentered: true,
      nzClassName: "texera-add-widget-modal",
      nzMaskClosable: false,
    });
  }

  removeWidget(widgetId: string): void {
    if (!this.dashboard) return;
    this.dashboardService.removeWidget(this.dashboard.id, widgetId);
  }

  startMove(event: MouseEvent, w: DashboardWidget): void {
    if (this.mode !== "edit") return;
    if ((event.target as HTMLElement).closest(".widget-controls, .resize-handle")) return;
    event.preventDefault();
    this.drag = {
      kind: "move",
      widgetId: w.id,
      startMouseX: event.clientX,
      startMouseY: event.clientY,
      startWidgetX: w.layout.x,
      startWidgetY: w.layout.y,
      moved: false,
    };
    document.body.classList.add("dashboard-dragging");
  }

  startResize(event: MouseEvent, w: DashboardWidget): void {
    if (this.mode !== "edit") return;
    event.preventDefault();
    event.stopPropagation();
    this.drag = {
      kind: "resize",
      widgetId: w.id,
      startMouseX: event.clientX,
      startMouseY: event.clientY,
      startWidth: w.layout.width,
      startHeight: w.layout.height,
    };
    document.body.classList.add("dashboard-dragging");
  }

  @HostListener("document:mousemove", ["$event"])
  onMouseMove(event: MouseEvent): void {
    if (!this.drag || !this.dashboard) return;
    const w = this.dashboard.widgets.find(x => x.id === this.drag!.widgetId);
    if (!w) return;

    if (this.drag.kind === "move") {
      const dx = event.clientX - this.drag.startMouseX;
      const dy = event.clientY - this.drag.startMouseY;
      const newX = Math.max(0, this.drag.startWidgetX + dx);
      const newY = Math.max(0, this.drag.startWidgetY + dy);
      if (newX === w.layout.x && newY === w.layout.y) return;
      this.drag.moved = true;
      this.dashboardService.updateLayout(this.dashboard.id, w.id, {
        ...w.layout,
        x: newX,
        y: newY,
      });
    } else {
      const dw = event.clientX - this.drag.startMouseX;
      const dh = event.clientY - this.drag.startMouseY;
      const newWidth = Math.max(MIN_WIDTH, this.drag.startWidth + dw);
      const newHeight = Math.max(MIN_HEIGHT, this.drag.startHeight + dh);
      if (newWidth === w.layout.width && newHeight === w.layout.height) return;
      this.dashboardService.updateLayout(this.dashboard.id, w.id, {
        ...w.layout,
        width: newWidth,
        height: newHeight,
      });
    }
  }

  @HostListener("document:mouseup")
  onMouseUp(): void {
    if (this.drag) {
      this.drag = null;
      document.body.classList.remove("dashboard-dragging");
    }
  }

  trackByWidgetId(_index: number, w: DashboardWidget): string {
    return w.id;
  }

  /** Memoize styles keyed on widget id + layout signature so [ngStyle]
   *  receives a referentially-stable object when nothing has moved. */
  private styleCache = new Map<string, { sig: string; style: { [k: string]: string } }>();

  widgetStyle(w: DashboardWidget): { [k: string]: string } {
    const sig = `${w.layout.x}|${w.layout.y}|${w.layout.width}|${w.layout.height}`;
    const cached = this.styleCache.get(w.id);
    if (cached && cached.sig === sig) {
      return cached.style;
    }
    const style: { [k: string]: string } = {
      left: `${w.layout.x}px`,
      top: `${w.layout.y}px`,
      width: `${w.layout.width}px`,
      height: `${w.layout.height}px`,
    };
    this.styleCache.set(w.id, { sig, style });
    return style;
  }

  /** The canvas grows to accommodate the lowest-positioned widget. */
  gridHeight(): number {
    if (!this.dashboard) return 600;
    let maxBottom = 0;
    for (const w of this.dashboard.widgets) {
      maxBottom = Math.max(maxBottom, w.layout.y + w.layout.height);
    }
    return Math.max(600, maxBottom + 80);
  }
}
