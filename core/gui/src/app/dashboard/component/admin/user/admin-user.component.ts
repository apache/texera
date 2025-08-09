/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Component, ElementRef, OnInit, ViewChild } from "@angular/core";
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
  editAttribute: string = "";
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

  @ViewChild("nameInput") nameInputRef?: ElementRef<HTMLInputElement>;
  @ViewChild("emailInput") emailInputRef?: ElementRef<HTMLInputElement>;
  @ViewChild("commentTextarea") commentTextareaRef?: ElementRef<HTMLTextAreaElement>;

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
    this.startEdit(user, "role");
    this.editRole = role;
    this.saveEdit();
  }

  addUser(): void {
    this.adminUserService
      .addUser()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.ngOnInit());
  }

  startEdit(user: User, attribute: string): void {
    this.editUid = user.uid;
    this.editAttribute = attribute;
    this.editName = user.name;
    this.editEmail = user.email;
    this.editRole = user.role;
    this.editComment = user.comment;

    setTimeout(() => {
      if (attribute === "name" && this.nameInputRef) {
        const input = this.nameInputRef.nativeElement;
        input.focus();
        input.setSelectionRange(input.value.length, input.value.length);
      } else if (attribute === "email" && this.emailInputRef) {
        const input = this.emailInputRef.nativeElement;
        input.focus();
        input.setSelectionRange(input.value.length, input.value.length);
      } else if (attribute === "comment" && this.commentTextareaRef) {
        const textarea = this.commentTextareaRef.nativeElement;
        textarea.focus();
        textarea.setSelectionRange(textarea.value.length, textarea.value.length);
      }
    }, 0);
  }

  saveEdit(): void {
    const originalUser = this.userList.find(u => u.uid === this.editUid);
    if (
      !originalUser ||
      (originalUser.name === this.editName &&
        originalUser.email === this.editEmail &&
        originalUser.comment === this.editComment &&
        originalUser.role === this.editRole)
    ) {
      this.stopEdit();
      return;
    }

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
    this.editAttribute = "";
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

  searchByComment(): void {
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

  private static readonly ACTIVE_WINDOW = 15 * 60 * 1000;

  isUserActive(user: User): boolean {
    if (!user.lastLogin) {
      return false;
    }

    const lastMs = user.lastLogin * 1000;
    return Date.now() - lastMs < AdminUserComponent.ACTIVE_WINDOW;
  }

  public filterByRole: NzTableFilterFn<User> = (list: string[], user: User) =>
    list.some(role => user.role.indexOf(role) !== -1);
}
