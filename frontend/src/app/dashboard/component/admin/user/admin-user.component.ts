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
import {
  NzTableFilterFn,
  NzTableSortFn,
  NzTableComponent,
  NzTheadComponent,
  NzTrDirective,
  NzTableCellDirective,
  NzThMeasureDirective,
  NzThAddOnComponent,
  NzFilterTriggerComponent,
  NzTbodyComponent,
} from "ng-zorro-antd/table";
import { NzModalService } from "ng-zorro-antd/modal";
import { NzMessageService } from "ng-zorro-antd/message";
import { AdminUserService } from "../../../service/admin/user/admin-user.service";
import { MilliSecond, Role, User } from "../../../../common/type/user";
import { UserService } from "../../../../common/service/user/user.service";
import { UserQuotaComponent } from "../../user/user-quota/user-quota.component";
import { FeedbackComponent } from "../../user/feedback/feedback.component";
import { FeedbackService } from "../../../service/user/feedback/feedback.service";
import { NzBadgeComponent } from "ng-zorro-antd/badge";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { replaceOneImmutable } from "../../../../common/util/array-utils";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzTagComponent } from "ng-zorro-antd/tag";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzDropdownMenuComponent } from "ng-zorro-antd/dropdown";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzInputDirective } from "ng-zorro-antd/input";
import { FormsModule } from "@angular/forms";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { NgFor, NgClass, NgIf, NgTemplateOutlet, DatePipe } from "@angular/common";
import { UserAvatarComponent } from "../../user/user-avatar/user-avatar.component";
import { NzSelectComponent, NzOptionComponent } from "ng-zorro-antd/select";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";

type AdminSearchField = "name" | "email" | "comment";

@UntilDestroy()
@Component({
  templateUrl: "./admin-user.component.html",
  styleUrls: ["./admin-user.component.scss"],
  imports: [
    NzCardComponent,
    NzTagComponent,
    NzTableComponent,
    NzTheadComponent,
    NzTrDirective,
    NzTableCellDirective,
    NzThMeasureDirective,
    NzThAddOnComponent,
    NzFilterTriggerComponent,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzDropdownMenuComponent,
    NzSpaceCompactItemDirective,
    NzInputDirective,
    FormsModule,
    NzButtonComponent,
    NzWaveDirective,
    NzTbodyComponent,
    NgFor,
    UserAvatarComponent,
    NgClass,
    NgIf,
    NgTemplateOutlet,
    NzSelectComponent,
    NzOptionComponent,
    NzTooltipDirective,
    NzBadgeComponent,
    DatePipe,
  ],
})
export class AdminUserComponent implements OnInit {
  private readonly searchFields: ReadonlyArray<AdminSearchField> = ["name", "email", "comment"];
  readonly roleFilters = [
    { text: "INACTIVE", value: "INACTIVE" },
    { text: "REGULAR", value: "REGULAR" },
    { text: "ADMIN", value: "ADMIN" },
    { text: "RESTRICTED", value: "RESTRICTED" },
  ];
  userList: ReadonlyArray<User> = [];
  editUid: number = 0;
  editAttribute: string = "";
  editName: string = "";
  editEmail: string = "";
  editRole: Role = Role.REGULAR;
  editComment: string = "";
  searchValue: Record<AdminSearchField, string> = { name: "", email: "", comment: "" };
  searchVisible: Record<AdminSearchField, boolean> = { name: false, email: false, comment: false };
  listOfDisplayUser = [...this.userList];
  currentUid: number | undefined = 0;
  feedbackCounts = new Map<number, number>();

  @ViewChild("nameInput") nameInputRef?: ElementRef<HTMLInputElement>;
  @ViewChild("emailInput") emailInputRef?: ElementRef<HTMLInputElement>;
  @ViewChild("commentTextarea") commentTextareaRef?: ElementRef<HTMLTextAreaElement>;

  constructor(
    private adminUserService: AdminUserService,
    private userService: UserService,
    private modalService: NzModalService,
    private messageService: NzMessageService,
    private config: GuiConfigService,
    private feedbackService: FeedbackService
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
    this.loadFeedbackCounts();
  }

  loadFeedbackCounts(): void {
    this.feedbackService
      .getFeedbackCounts()
      .pipe(untilDestroyed(this))
      .subscribe(counts => {
        this.feedbackCounts = new Map(counts.map(c => [c.uid, c.count]));
      });
  }

  getFeedbackCount(uid: number): number {
    return this.feedbackCounts.get(uid) ?? 0;
  }

  clickToViewFeedbacks(uid: number): void {
    this.modalService.create({
      nzContent: FeedbackComponent,
      nzData: { uid: uid },
      nzFooter: null,
      nzWidth: "60%",
      nzCentered: true,
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

    setTimeout(() => this.focusEditableElement(attribute), 0);
  }

  private focusEditableElement(attribute: string): void {
    const refMap: Record<string, ElementRef<HTMLInputElement | HTMLTextAreaElement> | undefined> = {
      name: this.nameInputRef,
      email: this.emailInputRef,
      comment: this.commentTextareaRef,
    };
    const element = refMap[attribute]?.nativeElement;
    if (!element) {
      return;
    }
    element.focus();
    element.setSelectionRange(element.value.length, element.value.length);
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
    // Edited User
    const updatedUser: User = {
      ...originalUser,
      name: this.editName,
      email: this.editEmail,
      comment: this.editComment,
      role: this.editRole,
    };

    this.stopEdit();

    this.adminUserService
      .updateUser(currentUid, this.editName, this.editEmail, this.editRole, this.editComment)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          // Update userList and listOfDisplayUser with updatedUser
          this.userList = [...replaceOneImmutable(this.userList, u => u.uid === currentUid, updatedUser)];
          this.listOfDisplayUser = [
            ...replaceOneImmutable(this.listOfDisplayUser, u => u.uid === currentUid, updatedUser),
          ];
        },
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

  private sortByStringField(selector: (user: User) => string | undefined): NzTableSortFn<User> {
    return (a: User, b: User) => {
      const compare = (selector(a) || "").localeCompare(selector(b) || "");
      return compare === 0 ? a.uid - b.uid : compare;
    };
  }

  // NzTableSortFn must order ascending: nz-table applies the result as-is for the 'ascend' sort
  // order and negates it for 'descend'. Reversing the operands here would render the column
  // backwards under the up caret.
  public sortByID: NzTableSortFn<User> = (a: User, b: User) => a.uid - b.uid;

  public sortByName: NzTableSortFn<User> = this.sortByStringField(u => u.name);

  public sortByEmail: NzTableSortFn<User> = this.sortByStringField(u => u.email);

  public sortByComment: NzTableSortFn<User> = this.sortByStringField(u => u.comment);

  public sortByRole: NzTableSortFn<User> = (a: User, b: User) => {
    const compare = a.role.localeCompare(b.role);
    return compare === 0 ? a.uid - b.uid : compare;
  };

  public sortByAccountCreation: NzTableSortFn<User> = (a: User, b: User) => {
    const compare = (a.accountCreation || 0) - (b.accountCreation || 0);
    return compare === 0 ? a.uid - b.uid : compare;
  };

  public sortByAffiliation: NzTableSortFn<User> = this.sortByStringField(u => u.affiliation);

  public sortByJoiningReason: NzTableSortFn<User> = this.sortByStringField(u => u.joiningReason);

  reset(): void {
    for (const field of this.searchFields) {
      this.searchValue[field] = "";
      this.searchVisible[field] = false;
    }
    this.listOfDisplayUser = [...this.userList];
  }

  private trimSearchValue(value: string | null | undefined): string {
    return (value ?? "").trim();
  }

  searchByField(field: AdminSearchField): void {
    this.searchVisible[field] = false;
    const trimmed = this.trimSearchValue(this.searchValue[field]);
    for (const candidate of this.searchFields) {
      this.searchValue[candidate] = candidate === field ? trimmed : "";
    }
    const query = trimmed.toLowerCase();
    this.listOfDisplayUser = this.userList.filter(user => (user[field] ?? "").toLowerCase().includes(query));
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

  isUserActive(user: User): boolean {
    if (!user.lastLogin) {
      return false;
    }
    // Active window set to active-time-in-minutes from gui.conf
    const active_window = this.config.env.activeTimeInMinutes * 60 * 1000;
    const lastMs = user.lastLogin * 1000;
    return Date.now() - lastMs < active_window;
  }

  getAccountCreation(user: User): MilliSecond {
    if (!user.accountCreation) {
      return 0;
    }
    return user.accountCreation * 1000;
  }

  // Active-before-inactive IS this column's ascending order, so the up caret surfaces the users
  // who are online now. Unlike the columns above there is no natural A-to-Z to reverse here: the
  // key is a boolean, and ranking active first is what makes the first click useful.
  sortByActive: NzTableSortFn<User> = (a: User, b: User) => {
    const aActive = this.isUserActive(a);
    const bActive = this.isUserActive(b);

    if (aActive === bActive) return a.uid - b.uid;
    return aActive ? -1 : 1;
  };

  public filterByRole: NzTableFilterFn<User> = (list: string[], user: User) =>
    list.some(role => user.role.indexOf(role) !== -1);
}
