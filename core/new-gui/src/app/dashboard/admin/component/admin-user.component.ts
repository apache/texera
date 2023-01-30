import { Component, OnInit } from "@angular/core";
import { AdminUserService } from "../service/admin-user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzTableSortFn } from "ng-zorro-antd/table";
import { Role, User } from "../../../common/type/user";

@UntilDestroy()
@Component({
  templateUrl: "./admin-user.component.html",
  styleUrls: ["./admin-user.component.scss"],
})
export class AdminUserComponent implements OnInit {
  userList: ReadonlyArray<User> = [];
  editUid: number = 0;
  editName: string = "";
  editEmail: string = "";
  editRole: Role = Role.REGULAR;

  constructor(private adminUserService: AdminUserService) {}

  ngOnInit() {
    this.adminUserService
      .getUserList()
      .pipe(untilDestroyed(this))
      .subscribe(userList => (this.userList = userList));
  }
  public updateRole(user: User, role: Role): void {
    this.startEdit(user);
    this.editRole = role;
    this.stopEdit();
  }

  deleteUser(uid: number): void {
    this.adminUserService
      .deleteUser(uid)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.ngOnInit());
  }

  addUser(): void {
    Date.now();
    this.adminUserService
      .deleteUser(1)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.ngOnInit());
  }

  startEdit(user: User): void {
    this.editUid = user.uid;
    this.editName = user.name;
    this.editEmail = user.email;
    this.editRole = user.role;
  }

  stopEdit(): void {
    const currentUid = this.editUid;
    this.editUid = 0;
    this.adminUserService
      .updateUser(currentUid, this.editName, this.editEmail, this.editRole)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.ngOnInit());
  }

  public sortByID: NzTableSortFn<User> = (a: User, b: User) => b.uid - a.uid;
  public sortByName: NzTableSortFn<User> = (a: User, b: User) => b.name.localeCompare(a.name);
  public sortByEmail: NzTableSortFn<User> = (a: User, b: User) => b.email.localeCompare(a.email);
  public sortByRole: NzTableSortFn<User> = (a: User, b: User) => b.role.localeCompare(a.role);
}
