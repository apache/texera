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

const COLS = 12;
const ROW_HEIGHT = 80;
const GUTTER = 12;

type DragMode = { kind: "move"; widgetId: string; offsetX: number; offsetY: number }
  | { kind: "resize"; widgetId: string; startW: number; startH: number; startX: number; startY: number };

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

  readonly COLS = COLS;
  readonly ROW_HEIGHT = ROW_HEIGHT;
  readonly GUTTER = GUTTER;

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
      onAdd: widgets => {
        if (!this.dashboard) return;
        this.dashboardService.addWidgets(this.dashboard.id, widgets);
      },
    };
    this.modal.create({
      nzContent: AddWidgetModalComponent,
      nzData: data,
      nzFooter: null,
      nzWidth: 720,
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
      offsetX: event.clientX,
      offsetY: event.clientY,
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
      startW: w.layout.w,
      startH: w.layout.h,
      startX: event.clientX,
      startY: event.clientY,
    };
    document.body.classList.add("dashboard-dragging");
  }

  @HostListener("document:mousemove", ["$event"])
  onMouseMove(event: MouseEvent): void {
    if (!this.drag || !this.dashboard || !this.gridEl) return;

    const colWidth = this.colPixelWidth();
    if (this.drag.kind === "move") {
      const dxPx = event.clientX - this.drag.offsetX;
      const dyPx = event.clientY - this.drag.offsetY;
      const dx = Math.round(dxPx / colWidth);
      const dy = Math.round(dyPx / (ROW_HEIGHT + GUTTER));
      if (dx === 0 && dy === 0) return;
      const w = this.dashboard.widgets.find(x => x.id === this.drag!.widgetId);
      if (!w) return;
      const newX = Math.max(0, Math.min(COLS - w.layout.w, w.layout.x + dx));
      const newY = Math.max(0, w.layout.y + dy);
      if (newX !== w.layout.x || newY !== w.layout.y) {
        const layout: WidgetLayout = { ...w.layout, x: newX, y: newY };
        this.dashboardService.updateLayout(this.dashboard.id, w.id, layout);
        if (this.drag.kind === "move") {
          this.drag.offsetX = event.clientX;
          this.drag.offsetY = event.clientY;
        }
      }
    } else if (this.drag.kind === "resize") {
      const dxPx = event.clientX - this.drag.startX;
      const dyPx = event.clientY - this.drag.startY;
      const dw = Math.round(dxPx / colWidth);
      const dh = Math.round(dyPx / (ROW_HEIGHT + GUTTER));
      const w = this.dashboard.widgets.find(x => x.id === this.drag!.widgetId);
      if (!w) return;
      const newW = Math.max(2, Math.min(COLS - w.layout.x, this.drag.startW + dw));
      const newH = Math.max(2, this.drag.startH + dh);
      if (newW !== w.layout.w || newH !== w.layout.h) {
        const layout: WidgetLayout = { ...w.layout, w: newW, h: newH };
        this.dashboardService.updateLayout(this.dashboard.id, w.id, layout);
      }
    }
  }

  @HostListener("document:mouseup")
  onMouseUp(): void {
    if (this.drag) {
      this.drag = null;
      document.body.classList.remove("dashboard-dragging");
    }
  }

  private colPixelWidth(): number {
    if (!this.gridEl) return 100;
    const totalWidth = this.gridEl.nativeElement.clientWidth;
    return (totalWidth - GUTTER * (COLS - 1)) / COLS;
  }

  widgetStyle(w: DashboardWidget): { [k: string]: string } {
    const colWidth = `calc((100% - ${GUTTER * (COLS - 1)}px) / ${COLS})`;
    return {
      left: `calc(${w.layout.x} * (${colWidth} + ${GUTTER}px))`,
      top: `${w.layout.y * (ROW_HEIGHT + GUTTER)}px`,
      width: `calc(${w.layout.w} * (${colWidth} + ${GUTTER}px) - ${GUTTER}px)`,
      height: `${w.layout.h * ROW_HEIGHT + (w.layout.h - 1) * GUTTER}px`,
    };
  }

  gridHeight(): number {
    if (!this.dashboard) return 600;
    let maxBottom = 0;
    for (const w of this.dashboard.widgets) {
      maxBottom = Math.max(maxBottom, w.layout.y + w.layout.h);
    }
    return Math.max(8, maxBottom + 1) * (ROW_HEIGHT + GUTTER);
  }
}
