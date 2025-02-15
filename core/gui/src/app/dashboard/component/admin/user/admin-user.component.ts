import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzTableFilterFn, NzTableSortFn } from "ng-zorro-antd/table";
import { NzModalService } from "ng-zorro-antd/modal";
import { NzMessageService } from "ng-zorro-antd/message";
import { AdminUserService } from "../../../service/admin/user/admin-user.service";
import { Role, User } from "../../../../common/type/user";
import { UserService } from "../../../../common/service/user/user.service";
import { UserQuotaComponent } from "../../user/user-quota/user-quota.component";

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
  editComment: string = "";
  nameSearchValue: string = "";
  emailSearchValue: string = "";
  commentSearchValue: string = "";
  nameSearchVisible = false;
  emailSearchVisible = false;
  commentSearchVisible = false;
  listOfDisplayUser = [...this.userList];
  currentUid: number | undefined = 0;

  constructor(
    private adminUserService: AdminUserService,
    private userService: UserService,
    private modalService: NzModalService,
    private messageService: NzMessageService
  ) {
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

  public updateComment(user: User, comment: string){
    this.startEdit(user);
    this.editComment = comment;
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
    this.editComment = user.comment;
  }

  saveEdit(): void {
    const currentUid = this.editUid;
    this.stopEdit();
    this.adminUserService
      .updateUser(currentUid, this.editName, this.editEmail, this.editRole, this.editComment)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.ngOnInit(),
        error: (err: unknown) => {
          const errorMessage = (err as any).error?.message || (err as Error).message;
          this.messageService.error(errorMessage);
        },
      });
  }

  stopEdit(): void {
    this.editUid = 0;
  }

  public sortByID: NzTableSortFn<User> = (a: User, b: User) => b.uid - a.uid;
  public sortByName: NzTableSortFn<User> = (a: User, b: User) => (b.name || "").localeCompare(a.name);
  public sortByEmail: NzTableSortFn<User> = (a: User, b: User) => (b.email || "").localeCompare(a.email);
  public sortByComment: NzTableSortFn<User> = (a: User, b: User) => (b.comment || "").localeCompare(a.comment);
  public sortByRole: NzTableSortFn<User> = (a: User, b: User) => b.role.localeCompare(a.role);

  reset(): void {
    this.nameSearchValue = "";
    this.emailSearchValue = "";
    this.commentSearchValue = "";
    this.nameSearchVisible = false;
    this.emailSearchVisible = false;
    this.commentSearchVisible = false;
    this.listOfDisplayUser = [...this.userList];
  }

  searchByName(): void {
    this.nameSearchVisible = false;
    this.listOfDisplayUser = this.userList.filter(user => (user.name || "").indexOf(this.nameSearchValue) !== -1);
  }

  searchByEmail(): void {
    this.emailSearchVisible = false;
    this.listOfDisplayUser = this.userList.filter(user => (user.email || "").indexOf(this.emailSearchValue) !== -1);
  }

  searchByComment(): void{
    this.commentSearchVisible = false;
    this.listOfDisplayUser = this.userList.filter(user => (user.comment || "").indexOf(this.commentSearchValue) !== -1);
  }

  clickToViewQuota(uid: number) {
    this.modalService.create({
      nzContent: UserQuotaComponent,
      nzData: { uid: uid },
      nzFooter: null,
      nzWidth: "80%",
      nzBodyStyle: { padding: "0" },
      nzCentered: true,
    });
  }

  public filterByRole: NzTableFilterFn<User> = (list: string[], user: User) =>
    list.some(role => user.role.indexOf(role) !== -1);
}
