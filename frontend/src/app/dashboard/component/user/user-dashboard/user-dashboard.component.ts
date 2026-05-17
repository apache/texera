/**
 * Dashboard list page — "My Dashboards" cards + create button.
 */

import { Component, OnInit } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { Router } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzPopconfirmDirective } from "ng-zorro-antd/popconfirm";
import { DashboardService } from "./dashboard.service";
import { Dashboard } from "./dashboard.types";
import { DASHBOARD_USER_DASHBOARD } from "../../../../app-routing.constant";

@UntilDestroy()
@Component({
  selector: "texera-user-dashboard",
  templateUrl: "./user-dashboard.component.html",
  styleUrls: ["./user-dashboard.component.scss"],
  imports: [
    CommonModule,
    FormsModule,
    NzButtonComponent,
    NzIconDirective,
    NzInputDirective,
    NzPopconfirmDirective,
  ],
})
export class UserDashboardComponent implements OnInit {
  dashboards: Dashboard[] = [];
  showCreateForm = false;
  newName = "";
  newDescription = "";

  constructor(
    private dashboardService: DashboardService,
    private router: Router
  ) {}

  ngOnInit(): void {
    this.dashboardService
      .list()
      .pipe(untilDestroyed(this))
      .subscribe(d => (this.dashboards = [...d].sort((a, b) => b.updatedAt - a.updatedAt)));
  }

  openCreate(): void {
    this.showCreateForm = true;
    this.newName = "";
    this.newDescription = "";
  }

  cancelCreate(): void {
    this.showCreateForm = false;
  }

  confirmCreate(): void {
    if (!this.newName.trim()) {
      return;
    }
    const d = this.dashboardService.create(this.newName, this.newDescription);
    this.showCreateForm = false;
    this.openEditor(d.id);
  }

  openEditor(id: string): void {
    this.router.navigate([DASHBOARD_USER_DASHBOARD, id, "edit"]);
  }

  remove(id: string): void {
    this.dashboardService.remove(id);
  }

  widgetCount(d: Dashboard): number {
    return d.widgets.length;
  }

  timeAgo(ts: number): string {
    const diff = Date.now() - ts;
    const m = Math.floor(diff / 60000);
    if (m < 1) return "just now";
    if (m < 60) return `${m}m ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    const d = Math.floor(h / 24);
    return `${d}d ago`;
  }
}
