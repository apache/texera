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

import { ComponentFixture, fakeAsync, inject, TestBed, tick } from "@angular/core/testing";
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

  const baseUser: User = {
    uid: 0,
    name: "",
    email: "",
    role: Role.REGULAR,
    comment: "",
    joiningReason: "",
  };
  const mk = (o: Partial<User>): User => ({ ...baseUser, ...o });

  describe("startEdit caret focus", () => {
    const fakeInput = (value: string) => ({ value, focus: vi.fn(), setSelectionRange: vi.fn() });

    it("focuses the name input and places the caret at the end after the setTimeout macrotask", fakeAsync(() => {
      const input = fakeInput("Alice");
      component.nameInputRef = { nativeElement: input } as any;

      component.startEdit(userA, "name");
      tick();

      expect(input.focus).toHaveBeenCalledTimes(1);
      expect(input.setSelectionRange).toHaveBeenCalledWith(5, 5);
    }));

    it("focuses the email input and places the caret at the end", fakeAsync(() => {
      const input = fakeInput("bob@example.com");
      component.emailInputRef = { nativeElement: input } as any;

      component.startEdit(userB, "email");
      tick();

      expect(input.focus).toHaveBeenCalledTimes(1);
      expect(input.setSelectionRange).toHaveBeenCalledWith(15, 15);
    }));

    it("focuses the comment textarea and places the caret at the end", fakeAsync(() => {
      const textarea = fakeInput("some comment");
      component.commentTextareaRef = { nativeElement: textarea } as any;

      component.startEdit(userA, "comment");
      tick();

      expect(textarea.focus).toHaveBeenCalledTimes(1);
      expect(textarea.setSelectionRange).toHaveBeenCalledWith(12, 12);
    }));

    it("does nothing on the timer when the matching input ref is absent", fakeAsync(() => {
      // nameInputRef is left undefined, so the name branch's `&& this.nameInputRef` guard is false.
      component.startEdit(userA, "name");
      expect(() => tick()).not.toThrow();
      expect(component.editAttribute).toBe("name");
    }));
  });

  describe("saveEdit error fallback", () => {
    it("falls back to err.message when err.error.message is absent", () => {
      const errorSpy = vi.spyOn(TestBed.inject(NzMessageService), "error").mockReturnValue({} as any);
      adminUserServiceSpy.updateUser.mockReturnValue(throwError(() => new Error("network down")));

      component.userList = [userA];
      component.editUid = userA.uid;
      component.editName = "Changed";
      component.editEmail = userA.email;
      component.editRole = userA.role;
      component.editComment = userA.comment;

      component.saveEdit();

      expect(errorSpy).toHaveBeenCalledWith("network down");
    });
  });

  describe("searchByName filtering", () => {
    it("filters the display list by name case-insensitively over populated data", () => {
      component.userList = [userA, userB];

      component.nameSearchValue = "ALI";
      component.searchByName();

      expect(component.listOfDisplayUser.length).toBe(1);
      expect(component.listOfDisplayUser[0].name).toBe("Alice");
      expect(component.nameSearchVisible).toBe(false);
    });

    it("trims a null name search value down to an empty string", () => {
      component.userList = [userA];

      component.nameSearchValue = null as any;
      component.searchByName();

      expect(component.nameSearchValue).toBe("");
      // empty query matches everything
      expect(component.listOfDisplayUser).toEqual([userA]);
    });

    it("tolerates null name/email/comment fields across all three searches", () => {
      const nullUser = mk({ uid: 3, name: null as any, email: null as any, comment: null as any });
      component.userList = [nullUser];

      component.nameSearchValue = "";
      component.searchByName();
      expect(component.listOfDisplayUser).toEqual([nullUser]);

      component.emailSearchValue = "";
      component.searchByEmail();
      expect(component.listOfDisplayUser).toEqual([nullUser]);

      component.commentSearchValue = "";
      component.searchByComment();
      expect(component.listOfDisplayUser).toEqual([nullUser]);
    });
  });

  describe("column sort comparators", () => {
    it("sortByID orders by descending uid", () => {
      expect(component.sortByID(mk({ uid: 1 }), mk({ uid: 2 }))).toBe(1);
      expect(component.sortByID(mk({ uid: 5 }), mk({ uid: 2 }))).toBe(-3);
    });

    it("sortByName compares names and falls back to uid on a tie", () => {
      expect(component.sortByName(mk({ uid: 1, name: "Alice" }), mk({ uid: 2, name: "Bob" }))).toBeGreaterThan(0);
      // equal names (both empty via null coalescing) -> uid tiebreak
      expect(component.sortByName(mk({ uid: 1, name: null as any }), mk({ uid: 2, name: null as any }))).toBe(-1);
    });

    it("sortByEmail compares emails and falls back to uid on a tie", () => {
      expect(component.sortByEmail(mk({ uid: 1, email: "a@x.com" }), mk({ uid: 2, email: "b@x.com" }))).toBeGreaterThan(
        0
      );
      expect(component.sortByEmail(mk({ uid: 1, email: null as any }), mk({ uid: 2, email: null as any }))).toBe(-1);
    });

    it("sortByComment compares comments and falls back to uid on a tie", () => {
      expect(component.sortByComment(mk({ uid: 1, comment: "aaa" }), mk({ uid: 2, comment: "bbb" }))).toBeGreaterThan(
        0
      );
      expect(component.sortByComment(mk({ uid: 1, comment: null as any }), mk({ uid: 2, comment: null as any }))).toBe(
        -1
      );
    });

    it("sortByRole compares roles and falls back to uid on a tie", () => {
      // "ADMIN".localeCompare("REGULAR") is negative
      expect(component.sortByRole(mk({ uid: 1, role: Role.REGULAR }), mk({ uid: 2, role: Role.ADMIN }))).toBeLessThan(
        0
      );
      expect(component.sortByRole(mk({ uid: 1, role: Role.ADMIN }), mk({ uid: 2, role: Role.ADMIN }))).toBe(-1);
    });

    it("sortByAccountCreation orders ascending and defaults missing values to 0", () => {
      expect(
        component.sortByAccountCreation(mk({ uid: 1, accountCreation: 1000 }), mk({ uid: 2, accountCreation: 3000 }))
      ).toBe(-2000);
      // both missing -> 0 - 0 -> uid tiebreak
      expect(
        component.sortByAccountCreation(
          mk({ uid: 1, accountCreation: undefined }),
          mk({ uid: 2, accountCreation: undefined })
        )
      ).toBe(-1);
    });

    it("sortByAffiliation compares affiliations and falls back to uid on a tie", () => {
      expect(
        component.sortByAffiliation(mk({ uid: 1, affiliation: "MIT" }), mk({ uid: 2, affiliation: "UCLA" }))
      ).toBeGreaterThan(0);
      expect(
        component.sortByAffiliation(mk({ uid: 1, affiliation: undefined }), mk({ uid: 2, affiliation: undefined }))
      ).toBe(-1);
    });

    it("sortByJoiningReason compares joining reasons and falls back to uid on a tie", () => {
      expect(
        component.sortByJoiningReason(
          mk({ uid: 1, joiningReason: "research" }),
          mk({ uid: 2, joiningReason: "teaching" })
        )
      ).toBeGreaterThan(0);
      expect(
        component.sortByJoiningReason(
          mk({ uid: 1, joiningReason: null as any }),
          mk({ uid: 2, joiningReason: null as any })
        )
      ).toBe(-1);
    });

    it("sortByActive ranks active users first and falls back to uid when equal", () => {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const active = mk({ uid: 1, lastLogin: nowSeconds });
      const inactive = mk({ uid: 2, lastLogin: undefined });

      // active before inactive
      expect(component.sortByActive(active, inactive)).toBe(-1);
      // inactive after active
      expect(component.sortByActive(inactive, active)).toBe(1);
      // both inactive -> uid tiebreak
      expect(component.sortByActive(mk({ uid: 2, lastLogin: undefined }), mk({ uid: 5, lastLogin: undefined }))).toBe(
        -3
      );
    });
  });

  describe("filterByRole", () => {
    it("matches when the user role is in the selected list", () => {
      expect(component.filterByRole([Role.ADMIN], mk({ role: Role.ADMIN }))).toBe(true);
      expect(component.filterByRole([Role.ADMIN, Role.REGULAR], mk({ role: Role.REGULAR }))).toBe(true);
    });

    it("does not match when the user role is absent from the selected list", () => {
      expect(component.filterByRole([Role.ADMIN], mk({ role: Role.REGULAR }))).toBe(false);
      expect(component.filterByRole([], mk({ role: Role.ADMIN }))).toBe(false);
    });
  });
});
