import { Component, OnInit } from "@angular/core";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { DashboardAdminUserEntry} from "../../../type/dashboard-admin-user-entry";
import { UserService } from "../../../../common/service/user/user.service";
import { AdminUserService } from "../../../service/admin-user/admin-user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import {NzTableSortFn} from "ng-zorro-antd/table";
@UntilDestroy()
@Component({
  selector: "texera-user-section",
  templateUrl: "./admin-user-section.component.html",
  styleUrls: ["./admin-user-section.component.scss"],
})

export class AdminUserSectionComponent implements OnInit {
  constructor(
    private modalService: NgbModal,
    private adminUserService: AdminUserService,
    private userService: UserService,
  ) {}

  ngOnInit() {
    this.registerRefresh();
  }
  public dashboardUserEntries: ReadonlyArray<DashboardAdminUserEntry> = [];

  public sort: NzTableSortFn<DashboardAdminUserEntry>=(a: DashboardAdminUserEntry, b: DashboardAdminUserEntry) => b.name.localeCompare(a.name);


  public getUserArray(): ReadonlyArray<DashboardAdminUserEntry> {
    return this.dashboardUserEntries;
  }


  public updatePermission(uid: number, permission: number): void {
    console.log(permission);
    this.adminUserService.updatePermission(uid,permission).pipe(untilDestroyed(this)).subscribe();
  }


  private registerRefresh(): void {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.userService.isLogin()) {
          this.refreshDashboardUserEntries();
        } else {
          this.clearDashboardUserEntries();
        }
      });
  }

  private refreshDashboardUserEntries(): void {
    this.adminUserService
      .retrieveUserList()
      .pipe(untilDestroyed(this))
      .subscribe(dashboardUserEntries => {
        this.dashboardUserEntries = dashboardUserEntries;
        this.adminUserService.updateUserFilesChangedEvent();
      });
  }

  private clearDashboardUserEntries(): void {
    this.dashboardUserEntries = [];
    this.adminUserService.updateUserFilesChangedEvent();
  }
}
