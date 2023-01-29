import { Component, OnInit } from "@angular/core";
import { AdminUserService } from "../service/admin-user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzTableSortFn } from "ng-zorro-antd/table";
import { User } from "../../../common/type/user";
@UntilDestroy()
@Component({
  templateUrl: "./admin-user.component.html",
  styleUrls: ["./admin-user.component.scss"],
})
export class AdminUserComponent implements OnInit {
  private userEntries: ReadonlyArray<User> = [];
  constructor(private adminUserService: AdminUserService) {}

  ngOnInit() {
    this.adminUserService
      .retrieveUserList()
      .pipe(untilDestroyed(this))
      .subscribe(userEntries => (this.userEntries = userEntries));
  }
  public getUserArray(): ReadonlyArray<User> {
    return this.userEntries;
  }
  public updateRole(uid: number, role: number): void {
    console.log(role);
    this.adminUserService.updateRole(uid, role).pipe(untilDestroyed(this)).subscribe();
  }
  public sort: NzTableSortFn<User> = (a: User, b: User) => b.name.localeCompare(a.name);
}
