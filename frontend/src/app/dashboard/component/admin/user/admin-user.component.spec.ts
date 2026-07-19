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

import { ComponentFixture, inject, TestBed } from "@angular/core/testing";
import { of, throwError } from "rxjs";
import { AdminUserComponent } from "./admin-user.component";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { AdminUserService } from "../../../service/admin/user/admin-user.service";
import { FeedbackService } from "../../../service/user/feedback/feedback.service";
import { Role, User } from "../../../../common/type/user";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { UserQuotaComponent } from "../../user/user-quota/user-quota.component";
import { FeedbackComponent } from "../../user/feedback/feedback.component";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { FormsModule } from "@angular/forms";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";
import { NzMessageService } from "ng-zorro-antd/message";
import { commonTestProviders } from "../../../../common/testing/test-utils";

const userA: User = {
  uid: 1,
  name: "Alice",
  email: "alice@example.com",
  role: Role.REGULAR,
  comment: "c1",
  joiningReason: "r1",
  accountCreation: 1000,
  lastLogin: 2000,
};

const userB: User = {
  uid: 2,
  name: "Bob",
  email: "bob@example.com",
  role: Role.ADMIN,
  comment: "c2",
  joiningReason: "r2",
  accountCreation: 3000,
};

describe("AdminUserComponent", () => {
  let component: AdminUserComponent;
  let fixture: ComponentFixture<AdminUserComponent>;

  let adminUserServiceSpy: {
    getUserList: ReturnType<typeof vi.fn>;
    addUser: ReturnType<typeof vi.fn>;
    updateUser: ReturnType<typeof vi.fn>;
  };
  let feedbackServiceSpy: { getFeedbackCounts: ReturnType<typeof vi.fn> };

  beforeEach(async () => {
    adminUserServiceSpy = {
      getUserList: vi.fn().mockReturnValue(of([])),
      addUser: vi.fn().mockReturnValue(of(undefined)),
      updateUser: vi.fn().mockReturnValue(of(undefined)),
    };
    feedbackServiceSpy = { getFeedbackCounts: vi.fn().mockReturnValue(of([])) };

    await TestBed.configureTestingModule({
      providers: [
        { provide: UserService, useClass: StubUserService },
        { provide: AdminUserService, useValue: adminUserServiceSpy },
        { provide: FeedbackService, useValue: feedbackServiceSpy },
        ...commonTestProviders,
      ],
      imports: [AdminUserComponent, FormsModule, HttpClientTestingModule, NzDropDownModule, NzModalModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminUserComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("should create", inject([HttpTestingController], () => {
    expect(component).toBeTruthy();
  }));

  it("should search email case-insensitively", () => {
    component.userList = [
      {
        name: "Alice",
        email: "alice@example.com",
        comment: "Needs review",
      } as any,
      {
        name: "Bob",
        email: "bob@example.com",
        comment: "Approved",
      } as any,
    ];

    component.emailSearchValue = "ALICE@EXAMPLE.COM";
    component.searchByEmail();

    expect(component.listOfDisplayUser.length).toBe(1);
    expect(component.listOfDisplayUser[0].email).toBe("alice@example.com");
  });

  it("should search comment case-insensitively", () => {
    component.userList = [
      {
        name: "Alice",
        email: "alice@example.com",
        comment: "Needs review",
      } as any,
      {
        name: "Bob",
        email: "bob@example.com",
        comment: "Approved",
      } as any,
    ];

    component.commentSearchValue = "NEEDS REVIEW";
    component.searchByComment();

    expect(component.listOfDisplayUser.length).toBe(1);
    expect(component.listOfDisplayUser[0].comment).toBe("Needs review");
  });

  it("should trim active email search value without lowercasing the stored input", () => {
    component.userList = [
      {
        name: "Alice",
        email: "alice@example.com",
        comment: "Needs review",
      } as any,
    ];

    component.emailSearchValue = "  ALICE@EXAMPLE.COM  ";
    component.searchByEmail();

    expect(component.emailSearchValue).toBe("ALICE@EXAMPLE.COM");
    expect(component.listOfDisplayUser.length).toBe(1);
    expect(component.listOfDisplayUser[0].email).toBe("alice@example.com");
  });

  it("should trim active comment search value without lowercasing the stored input", () => {
    component.userList = [
      {
        name: "Alice",
        email: "alice@example.com",
        comment: "Needs review",
      } as any,
    ];

    component.commentSearchValue = "  Needs review  ";
    component.searchByComment();

    expect(component.commentSearchValue).toBe("Needs review");
    expect(component.listOfDisplayUser.length).toBe(1);
    expect(component.listOfDisplayUser[0].comment).toBe("Needs review");
  });

  it("should clear inactive search values when searching by name", () => {
    component.emailSearchValue = "alice@example.com";
    component.commentSearchValue = "Needs review";

    component.nameSearchValue = "Alice";
    component.searchByName();

    expect(component.emailSearchValue).toBe("");
    expect(component.commentSearchValue).toBe("");
  });

  it("should clear inactive search values when searching by email", () => {
    component.nameSearchValue = "Alice";
    component.commentSearchValue = "Needs review";

    component.emailSearchValue = "bob@example.com";
    component.searchByEmail();

    expect(component.nameSearchValue).toBe("");
    expect(component.commentSearchValue).toBe("");
  });

  it("should clear inactive search values when searching by comment", () => {
    component.nameSearchValue = "Alice";
    component.emailSearchValue = "alice@example.com";

    component.commentSearchValue = "Approved";
    component.searchByComment();

    expect(component.nameSearchValue).toBe("");
    expect(component.emailSearchValue).toBe("");
  });

  it("addUser creates a user through the service and reloads the list", () => {
    const listCallsBefore = adminUserServiceSpy.getUserList.mock.calls.length;

    component.addUser();

    expect(adminUserServiceSpy.addUser).toHaveBeenCalledTimes(1);
    // the subscribe callback re-runs ngOnInit, which re-fetches the user list
    expect(adminUserServiceSpy.getUserList.mock.calls.length).toBe(listCallsBefore + 1);
  });

  it("startEdit loads the target row's values into the edit fields", () => {
    component.startEdit(userB, "email");

    expect(component.editUid).toBe(userB.uid);
    expect(component.editAttribute).toBe("email");
    expect(component.editName).toBe(userB.name);
    expect(component.editEmail).toBe(userB.email);
    expect(component.editRole).toBe(userB.role);
    expect(component.editComment).toBe(userB.comment);
  });

  it("saveEdit persists a changed row and updates the local lists", () => {
    component.userList = [userA];
    component.listOfDisplayUser = [userA];
    component.editUid = userA.uid;
    component.editName = "Alice Updated";
    component.editEmail = userA.email;
    component.editRole = userA.role;
    component.editComment = userA.comment;

    component.saveEdit();

    expect(adminUserServiceSpy.updateUser).toHaveBeenCalledWith(
      userA.uid,
      "Alice Updated",
      userA.email,
      userA.role,
      userA.comment
    );
    expect(component.userList.find(u => u.uid === userA.uid)?.name).toBe("Alice Updated");
    expect(component.listOfDisplayUser.find(u => u.uid === userA.uid)?.name).toBe("Alice Updated");
    expect(component.editUid).toBe(0); // stopEdit ran
  });

  it("saveEdit is a no-op when nothing changed", () => {
    component.userList = [userA];
    component.editUid = userA.uid;
    component.editName = userA.name;
    component.editEmail = userA.email;
    component.editRole = userA.role;
    component.editComment = userA.comment;

    component.saveEdit();

    expect(adminUserServiceSpy.updateUser).not.toHaveBeenCalled();
    expect(component.editUid).toBe(0);
  });

  it("saveEdit surfaces a service error through the message service", () => {
    const errorSpy = vi.spyOn(TestBed.inject(NzMessageService), "error").mockReturnValue({} as any);
    adminUserServiceSpy.updateUser.mockReturnValue(throwError(() => ({ error: { message: "update failed" } })));

    component.userList = [userA];
    component.editUid = userA.uid;
    component.editName = "Changed";
    component.editEmail = userA.email;
    component.editRole = userA.role;
    component.editComment = userA.comment;

    component.saveEdit();

    expect(errorSpy).toHaveBeenCalledWith("update failed");
  });

  it("stopEdit clears the edit target", () => {
    component.editUid = 5;
    component.editAttribute = "name";

    component.stopEdit();

    expect(component.editUid).toBe(0);
    expect(component.editAttribute).toBe("");
  });

  it("reset clears search state and restores the full display list", () => {
    component.userList = [userA, userB];
    component.nameSearchValue = "Alice";
    component.emailSearchValue = "alice@example.com";
    component.commentSearchValue = "c";
    component.nameSearchVisible = true;
    component.emailSearchVisible = true;
    component.commentSearchVisible = true;
    component.listOfDisplayUser = [userA];

    component.reset();

    expect(component.nameSearchValue).toBe("");
    expect(component.emailSearchValue).toBe("");
    expect(component.commentSearchValue).toBe("");
    expect(component.nameSearchVisible).toBe(false);
    expect(component.emailSearchVisible).toBe(false);
    expect(component.commentSearchVisible).toBe(false);
    expect(component.listOfDisplayUser).toEqual([userA, userB]);
  });

  it("updateRole edits only the role and persists it", () => {
    component.userList = [userA];
    component.listOfDisplayUser = [userA];

    component.updateRole(userA, Role.ADMIN);

    expect(adminUserServiceSpy.updateUser).toHaveBeenCalledWith(
      userA.uid,
      userA.name,
      userA.email,
      Role.ADMIN,
      userA.comment
    );
    expect(component.userList.find(u => u.uid === userA.uid)?.role).toBe(Role.ADMIN);
  });

  it("isUserActive reflects the recent-login window", () => {
    // Derive the boundary from the configured window so the test stays correct if the default changes.
    const windowSeconds = TestBed.inject(GuiConfigService).env.activeTimeInMinutes * 60;
    const nowSeconds = Math.floor(Date.now() / 1000);
    // just inside the active window
    expect(component.isUserActive({ ...userA, lastLogin: nowSeconds - Math.floor(windowSeconds / 2) } as User)).toBe(
      true
    );
    // just outside the active window
    expect(component.isUserActive({ ...userA, lastLogin: nowSeconds - (windowSeconds + 60) } as User)).toBe(false);
    expect(component.isUserActive({ ...userA, lastLogin: undefined } as User)).toBe(false);
  });

  it("getAccountCreation converts seconds to milliseconds and defaults to 0", () => {
    expect(component.getAccountCreation({ ...userA, accountCreation: 5 } as User)).toBe(5000);
    expect(component.getAccountCreation({ ...userA, accountCreation: undefined } as User)).toBe(0);
  });

  it("loadFeedbackCounts caches counts read back by getFeedbackCount", () => {
    feedbackServiceSpy.getFeedbackCounts.mockReturnValue(
      of([
        { uid: 1, count: 3 },
        { uid: 2, count: 0 },
      ])
    );

    component.loadFeedbackCounts();

    expect(component.getFeedbackCount(1)).toBe(3);
    expect(component.getFeedbackCount(2)).toBe(0);
    expect(component.getFeedbackCount(999)).toBe(0); // absent uid defaults to 0
  });

  it("clickToViewFeedbacks opens the feedback modal for the given uid", () => {
    const createSpy = vi.spyOn(TestBed.inject(NzModalService), "create").mockReturnValue({} as any);

    component.clickToViewFeedbacks(7);

    expect(createSpy).toHaveBeenCalledTimes(1);
    const config = createSpy.mock.calls[0][0];
    expect(config.nzContent).toBe(FeedbackComponent);
    expect(config.nzData).toEqual({ uid: 7 });
  });

  it("clickToViewQuota opens the quota modal for the given uid", () => {
    const createSpy = vi.spyOn(TestBed.inject(NzModalService), "create").mockReturnValue({} as any);

    component.clickToViewQuota(9);

    expect(createSpy).toHaveBeenCalledTimes(1);
    const config = createSpy.mock.calls[0][0];
    expect(config.nzContent).toBe(UserQuotaComponent);
    expect(config.nzData).toEqual({ uid: 9 });
  });
});
