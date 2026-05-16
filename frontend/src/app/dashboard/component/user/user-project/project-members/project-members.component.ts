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

import { Component, Input, OnChanges, SimpleChanges } from "@angular/core";
import { FormsModule } from "@angular/forms";
import { NgFor, NgIf } from "@angular/common";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { HttpErrorResponse } from "@angular/common/http";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzPopconfirmDirective } from "ng-zorro-antd/popconfirm";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { ShareAccessService } from "../../../../service/user/share-access/share-access.service";
import { Privilege, ShareAccess } from "../../../../type/share-access.interface";
import { UserService } from "../../../../../common/service/user/user.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";

type Role = "Owner" | "Editor" | "Viewer";

interface MemberRow {
  email: string;
  name: string;
  privilege: Privilege | "OWNER";
  role: Role;
  color: string;
  initial: string;
  isOwner: boolean;
  isYou: boolean;
}

const PALETTE = [
  "#ff85c0",
  "#ff8c50",
  "#bae637",
  "#36cfc9",
  "#9254de",
  "#5b8def",
  "#f5a623",
  "#13c2c2",
  "#eb2f96",
];

@UntilDestroy()
@Component({
  selector: "texera-project-members",
  templateUrl: "./project-members.component.html",
  styleUrls: ["./project-members.component.scss"],
  imports: [
    NgIf,
    NgFor,
    FormsModule,
    NzButtonComponent,
    NzIconDirective,
    NzPopconfirmDirective,
    NzTooltipDirective,
  ],
})
export class ProjectMembersComponent implements OnChanges {
  @Input() pid?: number;

  public inviteEmail = "";
  public inviteAccessLevel: "WRITE" | "READ" = "WRITE";
  public members: MemberRow[] = [];
  public ownerEmail = "";
  public ownerName = "";
  public currentEmail?: string;
  public loading = false;

  constructor(
    private shareAccessService: ShareAccessService,
    private userService: UserService,
    private notificationService: NotificationService
  ) {
    this.currentEmail = this.userService.getCurrentUser()?.email;
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["pid"]) {
      this.refresh();
    }
  }

  get hasWriteAccess(): boolean {
    if (!this.currentEmail) return false;
    if (this.currentEmail === this.ownerEmail) return true;
    const me = this.members.find(m => m.email === this.currentEmail);
    return me?.privilege === Privilege.WRITE;
  }

  public refresh(): void {
    if (typeof this.pid !== "number") return;
    const pid = this.pid;
    this.loading = true;
    this.shareAccessService
      .getOwner("project", pid)
      .pipe(untilDestroyed(this))
      .subscribe(name => {
        this.ownerName = name;
        this.ownerEmail = name; // backend returns email-as-name for owner endpoint
      });
    this.shareAccessService
      .getAccessList("project", pid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: list => {
          this.members = this.buildMemberRows(list);
          this.loading = false;
        },
        error: () => {
          this.members = [];
          this.loading = false;
        },
      });
  }

  public invite(): void {
    const email = this.inviteEmail.trim();
    if (!email || typeof this.pid !== "number") return;
    if (!this.isValidEmail(email)) {
      this.notificationService.error(`"${email}" is not a valid email address.`);
      return;
    }
    this.shareAccessService
      .grantAccess("project", this.pid, email, this.inviteAccessLevel)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.notificationService.success(`Invited ${email}.`);
          this.inviteEmail = "";
          this.refresh();
        },
        error: (err: unknown) => {
          if (err instanceof HttpErrorResponse) {
            this.notificationService.error(err.error?.message ?? `Failed to invite ${email}.`);
          } else {
            this.notificationService.error(`Failed to invite ${email}.`);
          }
        },
      });
  }

  public remove(member: MemberRow): void {
    if (typeof this.pid !== "number" || member.isOwner) return;
    this.shareAccessService
      .revokeAccess("project", this.pid, member.email)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.notificationService.success(`Removed ${member.email} from this project.`);
          this.refresh();
        },
        error: (err: unknown) => {
          if (err instanceof HttpErrorResponse) {
            this.notificationService.error(err.error?.message ?? "Failed to remove member.");
          }
        },
      });
  }

  public changeRole(member: MemberRow, newPrivilege: "WRITE" | "READ"): void {
    if (typeof this.pid !== "number" || member.isOwner) return;
    if (member.privilege === newPrivilege) return;
    this.shareAccessService
      .grantAccess("project", this.pid, member.email, newPrivilege)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.refresh(),
        error: () => this.refresh(),
      });
  }

  public trackMember = (_: number, m: MemberRow) => m.email;

  private buildMemberRows(list: ReadonlyArray<ShareAccess>): MemberRow[] {
    const ownerRow: MemberRow = {
      email: this.ownerEmail,
      name: this.ownerName || this.ownerEmail,
      privilege: "OWNER",
      role: "Owner",
      color: colorFor(this.ownerEmail),
      initial: initialOf(this.ownerName || this.ownerEmail),
      isOwner: true,
      isYou: this.currentEmail === this.ownerEmail,
    };
    const others: MemberRow[] = list.map(entry => ({
      email: entry.email,
      name: entry.name || entry.email,
      privilege: entry.privilege,
      role: privilegeToRole(entry.privilege),
      color: colorFor(entry.email),
      initial: initialOf(entry.name || entry.email),
      isOwner: false,
      isYou: this.currentEmail === entry.email,
    }));
    return [ownerRow, ...others];
  }

  private isValidEmail(email: string): boolean {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  }
}

function privilegeToRole(p: Privilege): Role {
  if (p === Privilege.WRITE) return "Editor";
  return "Viewer";
}

function initialOf(s: string): string {
  return (s || "?").trim().charAt(0).toUpperCase() || "?";
}

function colorFor(s: string): string {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = (h * 31 + s.charCodeAt(i)) >>> 0;
  }
  return PALETTE[h % PALETTE.length];
}
