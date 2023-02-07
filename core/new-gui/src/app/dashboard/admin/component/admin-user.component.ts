import { Component, OnInit } from "@angular/core";
import { AdminUserService } from "../service/admin-user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzTableSortFn, NzTableFilterFn } from "ng-zorro-antd/table";
import { Role, User } from "../../../common/type/user";
import { UserService } from "../../../common/service/user/user.service";

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

  nameSearchValue: string = "";
  emailSearchValue: string = "";
  nameSearchVisible = false;
  emailSearchVisible = false;
  listOfDisplayUser = [...this.userList];
  currentUid: number | undefined = 0;

  constructor(private adminUserService: AdminUserService, private userService: UserService) {
    this.currentUid = this.userService.getCurrentUser()?.uid;
  }

  ngOnInit() {
    this.adminUserService
      .getUserList()
      .pipe(untilDestroyed(this))
      .subscribe(userList => {
        this.userList = userList;
        this.reset();
      });
  }

  public updateRole(user: User, role: Role): void {
    this.startEdit(user);
    this.editRole = role;
    this.saveEdit();
  }

  addUser(): void {
    this.adminUserService
      .addUser()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.ngOnInit());
  }

  startEdit(user: User): void {
    this.editUid = user.uid;
    this.editName = user.name;
    this.editEmail = user.email;
    this.editRole = user.role;
  }

  saveEdit(): void {
    const currentUid = this.editUid;
    this.stopEdit();
    this.adminUserService
      .updateUser(currentUid, this.editName, this.editEmail, this.editRole)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.ngOnInit());
  }

  stopEdit(): void {
    this.editUid = 0;
  }

  public sortByID: NzTableSortFn<User> = (a: User, b: User) => b.uid - a.uid;
  public sortByName: NzTableSortFn<User> = (a: User, b: User) => b.name.localeCompare(a.name);
  public sortByEmail: NzTableSortFn<User> = (a: User, b: User) => b.email.localeCompare(a.email);
  public sortByRole: NzTableSortFn<User> = (a: User, b: User) => b.role.localeCompare(a.role);

  reset(): void {
    this.nameSearchValue = "";
    this.emailSearchValue = "";
    this.nameSearchVisible = false;
    this.emailSearchVisible = false;
    this.listOfDisplayUser = [...this.userList];
  }

  searchByName(): void {
    this.nameSearchVisible = false;
    this.listOfDisplayUser = this.userList.filter((user: User) => user.name.indexOf(this.nameSearchValue) !== -1);
  }

  searchByEmail(): void {
    this.emailSearchVisible = false;
    this.listOfDisplayUser = this.userList.filter(user => user.email.indexOf(this.emailSearchValue) !== -1);
  }

  public filterByRole: NzTableFilterFn<User> = (list: string[], user: User) =>
    list.some(role => user.role.indexOf(role) !== -1);
}
