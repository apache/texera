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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { of } from "rxjs";
import { NzModalCommentBoxComponent } from "./nz-modal-comment-box.component";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { UserService } from "../../../../common/service/user/user.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { User } from "../../../../common/type/user";
import { commonTestProviders } from "../../../../common/testing/test-utils";

const BOX_ID = "box-1";
const CREATION_TIME = "2026-01-01T00:00:00.000Z";

function makeUser(): User {
  return { uid: 1, name: "Alice", email: "alice@example.com", role: "REGULAR" } as User;
}

describe("NzModalCommentBoxComponent", () => {
  let addComment: ReturnType<typeof vi.fn>;
  let deleteComment: ReturnType<typeof vi.fn>;
  let editComment: ReturnType<typeof vi.fn>;

  async function createFixture(
    opts: { user?: User; comments?: unknown[] } = {}
  ): Promise<ComponentFixture<NzModalCommentBoxComponent>> {
    addComment = vi.fn();
    deleteComment = vi.fn();
    editComment = vi.fn();

    // The commentBox is a Yjs shared type; only .get('comments') (template) and
    // .get('commentBoxID').toJSON() (the id passed to the service) are exercised.
    const commentBox = {
      get: vi.fn((key: string) => (key === "comments" ? opts.comments ?? [] : { toJSON: () => BOX_ID })),
    };

    await TestBed.configureTestingModule({
      imports: [NzModalCommentBoxComponent, BrowserAnimationsModule],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: { commentBox } },
        { provide: WorkflowActionService, useValue: { addComment, deleteComment, editComment } },
        { provide: UserService, useValue: { userChanged: () => of(opts.user) } },
        { provide: NzModalRef, useValue: {} },
        { provide: NotificationService, useValue: { success: vi.fn(), error: vi.fn() } },
        ...commonTestProviders,
      ],
    }).compileComponents();

    return TestBed.createComponent(NzModalCommentBoxComponent);
  }

  it("should create and render a comment from the box", async () => {
    const fixture = await createFixture({
      user: makeUser(),
      comments: [{ content: "hi", creatorName: "Alice", creatorID: 1, creationTime: CREATION_TIME }],
    });
    fixture.detectChanges();

    expect(fixture.componentInstance).toBeTruthy();
    expect(fixture.nativeElement.querySelector(".modal-body")).toBeTruthy();
    expect(fixture.nativeElement.textContent).toContain("hi");
  });

  it("onClickAddComment adds the comment for the current user and clears the input", async () => {
    const user = makeUser();
    const fixture = await createFixture({ user });
    const component = fixture.componentInstance;
    component.inputValue = "Great work";

    component.onClickAddComment();

    expect(addComment).toHaveBeenCalledTimes(1);
    expect(addComment.mock.calls[0][0]).toMatchObject({
      content: "Great work",
      creatorName: user.name,
      creatorID: user.uid,
    });
    expect(addComment.mock.calls[0][1]).toBe(BOX_ID);
    expect(component.inputValue).toBe("");
    expect(component.submitting).toBe(false);
  });

  it("onClickAddComment does not add when there is no current user", async () => {
    const fixture = await createFixture({ user: undefined });
    const component = fixture.componentInstance;
    component.inputValue = "orphan comment";

    component.onClickAddComment();

    expect(addComment).not.toHaveBeenCalled();
    expect(component.inputValue).toBe(""); // input is still cleared regardless
  });

  it("deleteComment forwards to the service with the box id when a user is present", async () => {
    const fixture = await createFixture({ user: makeUser() });

    fixture.componentInstance.deleteComment(1, CREATION_TIME);

    expect(deleteComment).toHaveBeenCalledWith(1, CREATION_TIME, BOX_ID);
  });

  it("deleteComment is a no-op without a current user", async () => {
    const fixture = await createFixture({ user: undefined });

    fixture.componentInstance.deleteComment(1, CREATION_TIME);

    expect(deleteComment).not.toHaveBeenCalled();
  });

  it("editComment forwards the new content to the service and resets editValue", async () => {
    const fixture = await createFixture({ user: makeUser() });
    const component = fixture.componentInstance;
    component.editValue = "updated content";

    component.editComment(1, "Alice", CREATION_TIME);

    expect(editComment).toHaveBeenCalledWith(1, CREATION_TIME, BOX_ID, "updated content");
    expect(component.editValue).toBe("");
  });

  it("replyToComment appends a quoted mention to the input", async () => {
    const fixture = await createFixture({ user: makeUser() });
    const component = fixture.componentInstance;
    component.inputValue = "";

    component.replyToComment("Bob", "nice diagram");

    expect(component.inputValue).toBe('@Bob:"nice diagram"\n');
  });

  it("onKeyDown submits the comment on Ctrl/Cmd+Enter and ignores other keys", async () => {
    const fixture = await createFixture({ user: makeUser() });
    const component = fixture.componentInstance;
    const submitSpy = vi.spyOn(component, "onClickAddComment");

    component.onKeyDown(new KeyboardEvent("keydown", { key: "Enter", ctrlKey: true }));
    expect(submitSpy).toHaveBeenCalledTimes(1);

    component.onKeyDown(new KeyboardEvent("keydown", { key: "a", ctrlKey: true }));
    expect(submitSpy).toHaveBeenCalledTimes(1);
  });
});
