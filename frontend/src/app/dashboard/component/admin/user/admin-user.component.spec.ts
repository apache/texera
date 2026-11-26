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
import { By } from "@angular/platform-browser";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";
import { NzMessageService } from "ng-zorro-antd/message";
import { commonTestProviders } from "../../../../common/testing/test-utils";

const baseUser: User = {
  uid: 0,
  name: "",
  email: "",
  role: Role.REGULAR,
  comment: "",
  joiningReason: "",
};
const mk = (o: Partial<User>): User => ({ ...baseUser, ...o });

const userA: User = mk({
  uid: 1,
  name: "Alice",
  email: "alice@example.com",
  comment: "c1",
  joiningReason: "r1",
  accountCreation: 1000,
  lastLogin: 2000,
});

const userB: User = mk({
  uid: 2,
  name: "Bob",
  email: "bob@example.com",
  role: Role.ADMIN,
  comment: "c2",
  joiningReason: "r2",
  accountCreation: 3000,
});

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

  it("shows a placeholder tag only for placeholder accounts", () => {
    component.listOfDisplayUser = [
      { uid: 1, name: "test1", email: "test1@example.com", isPlaceholder: true } as any,
      { uid: 2, name: "test2", email: "test2@example.com" } as any,
    ];
    fixture.detectChanges();

    const tags: NodeListOf<HTMLElement> = fixture.nativeElement.querySelectorAll("nz-tag");
    const placeholderTags = Array.from(tags).filter(tag => tag.textContent?.includes("placeholder"));
    expect(placeholderTags.length).toBe(1);
    expect(placeholderTags[0].closest("tr")?.textContent).toContain("test1");
  });

  const searchUsers = (): User[] => [
    mk({ name: "Alice", email: "alice@example.com", comment: "Needs review" }),
    mk({ name: "Bob", email: "bob@example.com", comment: "Approved" }),
  ];

  (
    [
      ["email", "ALICE@EXAMPLE.COM", "alice@example.com"],
      ["comment", "NEEDS REVIEW", "Needs review"],
    ] as const
  ).forEach(([field, query, expected]) => {
    it(`should search ${field} case-insensitively`, () => {
      component.userList = searchUsers();
      component.searchValue[field] = query;
      component.searchByField(field);
      expect(component.listOfDisplayUser.length).toBe(1);
      expect(component.listOfDisplayUser[0][field]).toBe(expected);
    });
  });

  (
    [
      ["email", "  ALICE@EXAMPLE.COM  ", "ALICE@EXAMPLE.COM", "alice@example.com"],
      ["comment", "  Needs review  ", "Needs review", "Needs review"],
    ] as const
  ).forEach(([field, query, trimmed, expected]) => {
    it(`should trim active ${field} search value without lowercasing the stored input`, () => {
      component.userList = [searchUsers()[0]];
      component.searchValue[field] = query;
      component.searchByField(field);
      expect(component.searchValue[field]).toBe(trimmed);
      expect(component.listOfDisplayUser.length).toBe(1);
      expect(component.listOfDisplayUser[0][field]).toBe(expected);
    });
  });

  (
    [
      ["name", "Alice", "email", "alice@example.com", "comment", "Needs review"],
      ["email", "bob@example.com", "name", "Alice", "comment", "Needs review"],
      ["comment", "Approved", "name", "Alice", "email", "alice@example.com"],
    ] as const
  ).forEach(([field, query, f1, v1, f2, v2]) => {
    it(`should clear inactive search values when searching by ${field}`, () => {
      component.searchValue[f1] = v1;
      component.searchValue[f2] = v2;
      component.searchValue[field] = query;
      component.searchByField(field);
      expect(component.searchValue[f1]).toBe("");
      expect(component.searchValue[f2]).toBe("");
    });
  });

  function stageEdit(editName = userA.name): void {
    component.userList = [userA];
    component.editUid = userA.uid;
    component.editName = editName;
    component.editEmail = userA.email;
    component.editRole = userA.role;
    component.editComment = userA.comment;
  }

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
    stageEdit("Alice Updated");
    component.listOfDisplayUser = [userA];

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
    stageEdit();

    component.saveEdit();

    expect(adminUserServiceSpy.updateUser).not.toHaveBeenCalled();
    expect(component.editUid).toBe(0);
  });

  it("saveEdit surfaces a service error through the message service", () => {
    const errorSpy = vi.spyOn(TestBed.inject(NzMessageService), "error").mockReturnValue({} as any);
    adminUserServiceSpy.updateUser.mockReturnValue(throwError(() => ({ error: { message: "update failed" } })));
    stageEdit("Changed");

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
    component.searchValue.name = "Alice";
    component.searchValue.email = "alice@example.com";
    component.searchValue.comment = "c";
    component.searchVisible.name = true;
    component.searchVisible.email = true;
    component.searchVisible.comment = true;
    component.listOfDisplayUser = [userA];

    component.reset();

    expect(component.searchValue.name).toBe("");
    expect(component.searchValue.email).toBe("");
    expect(component.searchValue.comment).toBe("");
    expect(component.searchVisible.name).toBe(false);
    expect(component.searchVisible.email).toBe(false);
    expect(component.searchVisible.comment).toBe(false);
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

  (
    [
      ["feedback", 7, FeedbackComponent, () => component.clickToViewFeedbacks(7)],
      ["quota", 9, UserQuotaComponent, () => component.clickToViewQuota(9)],
    ] as const
  ).forEach(([kind, uid, expectedContent, openModal]) => {
    it(`clickToView${kind === "feedback" ? "Feedbacks" : "Quota"} opens the ${kind} modal for the given uid`, () => {
      const createSpy = vi.spyOn(TestBed.inject(NzModalService), "create").mockReturnValue({} as any);

      openModal();

      expect(createSpy).toHaveBeenCalledTimes(1);
      const config = createSpy.mock.calls[0][0];
      expect(config.nzContent).toBe(expectedContent);
      expect(config.nzData).toEqual({ uid });
    });
  });

  describe("startEdit caret focus", () => {
    const fakeInput = (value: string) => ({ value, focus: vi.fn(), setSelectionRange: vi.fn() });

    (
      [
        [
          "name",
          userA,
          "nameInputRef",
          "Alice",
          "focuses the name input and places the caret at the end after the setTimeout macrotask",
        ],
        ["email", userB, "emailInputRef", "bob@example.com", "focuses the email input and places the caret at the end"],
        [
          "comment",
          userA,
          "commentTextareaRef",
          "some comment",
          "focuses the comment textarea and places the caret at the end",
        ],
      ] as const
    ).forEach(([attribute, user, refKey, value, title]) => {
      it(
        title,
        fakeAsync(() => {
          const el = fakeInput(value);
          component[refKey] = { nativeElement: el } as any;

          component.startEdit(user, attribute);
          tick();

          expect(el.focus).toHaveBeenCalledTimes(1);
          expect(el.setSelectionRange).toHaveBeenCalledWith(value.length, value.length);
        })
      );
    });

    it("does nothing on the timer when the matching input ref is absent", fakeAsync(() => {
      // nameInputRef is left undefined, so there is no name element to focus.
      component.startEdit(userA, "name");
      expect(() => tick()).not.toThrow();
      expect(component.editAttribute).toBe("name");
    }));
  });

  describe("saveEdit error fallback", () => {
    it("falls back to err.message when err.error.message is absent", () => {
      const errorSpy = vi.spyOn(TestBed.inject(NzMessageService), "error").mockReturnValue({} as any);
      adminUserServiceSpy.updateUser.mockReturnValue(throwError(() => new Error("network down")));
      stageEdit("Changed");

      component.saveEdit();

      expect(errorSpy).toHaveBeenCalledWith("network down");
    });
  });

  describe("searchByName filtering", () => {
    it("filters the display list by name case-insensitively over populated data", () => {
      component.userList = [userA, userB];

      component.searchValue.name = "ALI";
      component.searchByField("name");

      expect(component.listOfDisplayUser.length).toBe(1);
      expect(component.listOfDisplayUser[0].name).toBe("Alice");
      expect(component.searchVisible.name).toBe(false);
    });

    it("trims a null name search value down to an empty string", () => {
      component.userList = [userA];

      component.searchValue.name = null as any;
      component.searchByField("name");

      expect(component.searchValue.name).toBe("");
      // empty query matches everything
      expect(component.listOfDisplayUser).toEqual([userA]);
    });

    it("tolerates null name/email/comment fields across all three searches", () => {
      const nullUser = mk({ uid: 3, name: null as any, email: null as any, comment: null as any });
      component.userList = [nullUser];

      component.searchValue.name = "";
      component.searchByField("name");
      expect(component.listOfDisplayUser).toEqual([nullUser]);

      component.searchValue.email = "";
      component.searchByField("email");
      expect(component.listOfDisplayUser).toEqual([nullUser]);

      component.searchValue.comment = "";
      component.searchByField("comment");
      expect(component.listOfDisplayUser).toEqual([nullUser]);
    });
  });

  /**
   * Every comparator here has to order its column *ascending*, because nz-table applies the
   * comparator's result as-is for 'ascend' and negates it for 'descend'. A comparator with
   * reversed operands therefore lights the up caret while rendering the column backwards. The
   * rendered-header tests further down pin the caret and the row order together per column.
   */
  describe("column sort comparators", () => {
    it("sortByID orders by ascending uid", () => {
      expect(component.sortByID(mk({ uid: 1 }), mk({ uid: 2 }))).toBe(-1);
      expect(component.sortByID(mk({ uid: 5 }), mk({ uid: 2 }))).toBe(3);
    });

    const stringSortCases = [
      ["sortByName", "name", "Alice", "Bob", null as any, "sortByName compares names and falls back to uid on a tie"],
      [
        "sortByEmail",
        "email",
        "a@x.com",
        "b@x.com",
        null as any,
        "sortByEmail compares emails and falls back to uid on a tie",
      ],
      [
        "sortByComment",
        "comment",
        "aaa",
        "bbb",
        null as any,
        "sortByComment compares comments and falls back to uid on a tie",
      ],
      [
        "sortByAffiliation",
        "affiliation",
        "MIT",
        "UCLA",
        undefined,
        "sortByAffiliation compares affiliations and falls back to uid on a tie",
      ],
      [
        "sortByJoiningReason",
        "joiningReason",
        "research",
        "teaching",
        null as any,
        "sortByJoiningReason compares joining reasons and falls back to uid on a tie",
      ],
    ] as const;
    const itSortsStrings = ([fn, field, low, high, empty, title]: (typeof stringSortCases)[number]) =>
      it(title, () => {
        expect(component[fn](mk({ uid: 1, [field]: low }), mk({ uid: 2, [field]: high }))).toBeLessThan(0);
        // equal values (empty via null/undefined coalescing) -> uid tiebreak
        expect(component[fn](mk({ uid: 1, [field]: empty }), mk({ uid: 2, [field]: empty }))).toBe(-1);
      });
    // Registered around the role and account-creation cases to keep the original test order.
    stringSortCases.slice(0, 3).forEach(itSortsStrings);

    it("sortByRole compares roles and falls back to uid on a tie", () => {
      // "REGULAR".localeCompare("ADMIN") is positive, so REGULAR sorts after ADMIN
      expect(
        component.sortByRole(mk({ uid: 1, role: Role.REGULAR }), mk({ uid: 2, role: Role.ADMIN }))
      ).toBeGreaterThan(0);
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

    stringSortCases.slice(3).forEach(itSortsStrings);

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

  /**
   * The tests above call the component's methods; these drive the table itself — the
   * per-column search dropdowns, the click-to-edit cells and the row actions — so a control
   * that loses its handler fails here.
   */
  describe("rendered table", () => {
    /** Seeds both lists, as loading the users does, and renders the rows. */
    function renderUsers(users: User[]): void {
      component.userList = [...users];
      component.listOfDisplayUser = [...users];
      fixture.detectChanges();
    }

    it("swaps a cell for an input when it is clicked, and saves on enter", () => {
      const saveSpy = vi.spyOn(component, "saveEdit").mockImplementation(() => {});
      renderUsers([userA]);

      const nameCell = fixture.nativeElement.querySelectorAll("tbody tr td")[2].querySelector(".container");
      nameCell.click();
      fixture.detectChanges();

      expect(component.editUid).toBe(userA.uid);
      expect(component.editAttribute).toBe("name");
      const input = fixture.nativeElement.querySelectorAll("tbody tr td")[2].querySelector("input");
      expect(input).not.toBeNull();

      input.value = "Alicia";
      input.dispatchEvent(new Event("input"));
      fixture.detectChanges();
      expect(component.editName).toBe("Alicia");

      // Only Enter commits; other keys leave the edit open.
      input.dispatchEvent(new KeyboardEvent("keydown", { key: "a", bubbles: true }));
      expect(saveSpy).not.toHaveBeenCalled();

      input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
      expect(saveSpy).toHaveBeenCalled();

      // Clicking away saves too, so a half-typed edit is not silently dropped.
      saveSpy.mockClear();
      fixture.nativeElement
        .querySelectorAll("tbody tr td")[2]
        .querySelector("div")
        .dispatchEvent(new Event("focusout", { bubbles: true }));

      expect(saveSpy).toHaveBeenCalled();
    });

    it("does the same for the email and comment cells, and saves when the cell loses focus", () => {
      const saveSpy = vi.spyOn(component, "saveEdit").mockImplementation(() => {});
      renderUsers([userA]);
      const cell = (col: number) => fixture.nativeElement.querySelectorAll("tbody tr td")[col];

      (
        [
          [3, "email", "input[type=email]", "alicia@example.com", "editEmail"],
          [6, "comment", "textarea", "reviewed", "editComment"],
        ] as const
      ).forEach(([col, attr, selector, value, editProp]) => {
        saveSpy.mockClear();
        cell(col).querySelector(".container").click();
        fixture.detectChanges();
        expect(component.editAttribute).toBe(attr);
        const input = cell(col).querySelector(selector);
        expect(input).not.toBeNull();
        input.value = value;
        input.dispatchEvent(new Event("input"));
        fixture.detectChanges();
        expect(component[editProp]).toBe(value);
        input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
        expect(saveSpy).toHaveBeenCalled();

        // Cleared between the two so each binding is what its own assertion proves.
        saveSpy.mockClear();
        cell(col)
          .querySelector("div")
          .dispatchEvent(new Event("focusout", { bubbles: true }));
        expect(saveSpy).toHaveBeenCalled();
      });
    });

    it("updates the role through the row's select", () => {
      const updateSpy = vi.spyOn(component, "updateRole").mockImplementation(() => {});
      renderUsers([userA]);

      fixture.debugElement.query(By.css("nz-select")).triggerEventHandler("ngModelChange", Role.ADMIN);

      expect(updateSpy).toHaveBeenCalledWith(expect.objectContaining({ uid: userA.uid }), Role.ADMIN);
    });

    it("opens the quota and feedback views from the row actions", () => {
      const quotaSpy = vi.spyOn(component, "clickToViewQuota").mockImplementation(() => {});
      const feedbackSpy = vi.spyOn(component, "clickToViewFeedbacks").mockImplementation(() => {});
      feedbackServiceSpy.getFeedbackCounts.mockReturnValue(of([{ uid: userA.uid, count: 3 }]));
      component.ngOnInit();
      renderUsers([userA]);

      const cells = fixture.nativeElement.querySelectorAll("tbody tr td");
      cells[8].querySelector("button").click();
      const feedbackButton = cells[9].querySelector("button");
      expect(feedbackButton.disabled).toBe(false);
      feedbackButton.click();

      expect(quotaSpy).toHaveBeenCalledWith(userA.uid);
      expect(feedbackSpy).toHaveBeenCalledWith(userA.uid);
    });

    it("disables the feedback button for a user with none", () => {
      renderUsers([userA]);

      const cells = fixture.nativeElement.querySelectorAll("tbody tr td");
      expect(cells[9].querySelector("button").disabled).toBe(true);
    });

    it("shows a creation date when there is one and a dash when there is not", () => {
      renderUsers([userA, { ...userB, accountCreation: undefined } as User]);

      const cells = fixture.nativeElement.querySelectorAll("tbody tr");
      // The date is rendered through a pipe in the runner's timezone, so only its presence
      // is asserted here, not the formatted value.
      const withDate = (cells[0].querySelectorAll("td")[10].textContent ?? "").trim();
      const withoutDate = (cells[1].querySelectorAll("td")[10].textContent ?? "").trim();
      expect(withDate).not.toBe("");
      expect(withDate).not.toBe("—");
      expect(withoutDate).toBe("—");
    });

    it("adds a user from the Add button", () => {
      const addSpy = vi.spyOn(component, "addUser").mockImplementation(() => {});
      renderUsers([userA]);

      const addButton = Array.from(fixture.nativeElement.querySelectorAll("button")).find(
        button => ((button as HTMLElement).textContent ?? "").trim() === "Add"
      ) as HTMLButtonElement;
      addButton.click();

      expect(addSpy).toHaveBeenCalled();
    });
  });

  /**
   * Drives the real sortable headers so that the caret nz-table lights and the order it renders are
   * observed together. Asserting only the row order would still pass against a comparator inverted
   * the other way — nz-table uses the comparator's result as-is for 'ascend' and negates it for
   * 'descend', so reversed operands produce a Z-to-A up caret. Every column below therefore pins
   * both halves in both directions.
   *
   * The four rows carry a distinct value in every sorted field, so no comparator ever reaches its
   * uid tiebreak and swapping any two rows is visible. The per-field orders are chosen so that each
   * column's ascending and descending sequence is unique across the whole table and differs from the
   * order the rows are supplied in — no column's expectation can be satisfied by another column's
   * sort, or by the table not sorting at all.
   */
  describe("sorted columns (rendered header)", () => {
    // The supplied order is deliberately none of the sequences asserted below - not uid order (which
    // would make the ID column's ascending leg pass without sorting anything) and not any other
    // column's ascending or descending order either. Reshuffling this needs the same check.
    const SORT_USERS: ReadonlyArray<User> = [
      mk({ uid: 4, name: "Cyd", email: "d@x.com", affiliation: "Aff-C", joiningReason: "reason-b", comment: "note-c", role: Role.ADMIN }), // prettier-ignore
      mk({ uid: 2, name: "Dov", email: "c@x.com", affiliation: "Aff-A", joiningReason: "reason-c", comment: "note-a", role: Role.REGULAR }), // prettier-ignore
      mk({ uid: 1, name: "Bea", email: "a@x.com", affiliation: "Aff-B", joiningReason: "reason-a", comment: "note-d", role: Role.INACTIVE }), // prettier-ignore
      mk({ uid: 3, name: "Ada", email: "b@x.com", affiliation: "Aff-D", joiningReason: "reason-d", comment: "note-b", role: Role.RESTRICTED }), // prettier-ignore
    ];
    const SUPPLIED_ORDER = [4, 2, 1, 3];

    /** Column header label -> the uids it must render top-to-bottom under the up (ascend) caret. */
    const ASCENDING_BY_COLUMN: ReadonlyArray<[label: string, ascendingUids: number[]]> = [
      ["ID", [1, 2, 3, 4]], // lowest uid (oldest account) first, like every other ascending column
      ["Name", [3, 1, 4, 2]], // Ada, Bea, Cyd, Dov
      ["Email", [1, 3, 2, 4]], // a@, b@, c@, d@
      ["Affiliation", [2, 1, 4, 3]], // Aff-A, Aff-B, Aff-C, Aff-D
      ["Joining Reason", [1, 4, 2, 3]], // reason-a, reason-b, reason-c, reason-d
      ["Comment", [2, 3, 4, 1]], // note-a, note-b, note-c, note-d
      ["User Role", [4, 1, 2, 3]], // ADMIN, INACTIVE, REGULAR, RESTRICTED
    ];

    /** `fixture.nativeElement` is `any`, so narrow it once for the DOM queries below. */
    const host = (): HTMLElement => fixture.nativeElement as HTMLElement;

    /** The uid cell of each rendered row, top to bottom. */
    function renderedUids(): number[] {
      return Array.from(host().querySelectorAll<HTMLElement>("tbody tr")).map(row =>
        Number((row.querySelectorAll("td")[1].textContent ?? "").trim())
      );
    }

    function sortableHeader(label: string): HTMLElement {
      const header = Array.from(host().querySelectorAll<HTMLElement>("thead th")).find(
        th => (th.querySelector(".ant-table-column-title")?.textContent ?? "").replace(/\s+/g, " ").trim() === label
      );
      if (!header) {
        throw new Error(`no sortable header titled "${label}"`);
      }
      return header;
    }

    function caretActive(header: HTMLElement, direction: "up" | "down"): boolean {
      const caret = header.querySelector(`.ant-table-column-sorter-${direction}`);
      if (!caret) {
        throw new Error(`header has no ${direction} caret`);
      }
      return caret.classList.contains("active");
    }

    function clickSort(header: HTMLElement): void {
      header.click();
      // nz-table republishes its sort operators on a macrotask (`delay(0)`), so a bare
      // detectChanges() would still read the previous ordering.
      vi.advanceTimersByTime(0);
      fixture.detectChanges();
    }

    ASCENDING_BY_COLUMN.forEach(([label, ascendingUids]) => {
      const descendingUids = [...ascendingUids].reverse();

      it(`sorts ${label} A-to-Z under the up caret and Z-to-A under the down caret`, () => {
        // The header listener runs outside Angular's test Zone. Control its timers without faking Date.
        vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
        try {
          component.userList = [...SORT_USERS];
          component.listOfDisplayUser = [...SORT_USERS];
          fixture.detectChanges();
          expect(renderedUids()).toEqual(SUPPLIED_ORDER);

          const header = sortableHeader(label);

          // nzSortDirections is ['ascend', 'descend'] on all of these headers, so the first click
          // selects 'ascend' and lights the up caret: the column must read A-to-Z.
          clickSort(header);
          expect(caretActive(header, "up")).toBe(true);
          expect(caretActive(header, "down")).toBe(false);
          expect(renderedUids()).toEqual(ascendingUids);

          clickSort(header);
          expect(caretActive(header, "down")).toBe(true);
          expect(caretActive(header, "up")).toBe(false);
          expect(renderedUids()).toEqual(descendingUids);
        } finally {
          vi.useRealTimers();
        }
      });
    });
  });
});
