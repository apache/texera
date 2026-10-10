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
  let lastFixture: ComponentFixture<NzModalCommentBoxComponent> | undefined;
  let createdEls: HTMLElement[] = [];

  // toggleEditInput / editComment locate their DOM targets by id
  // ("txarea"|"comment"|"editbtn" + creatorName + creationTime). Build real
  // elements so those document.getElementById lookups resolve.
  function makeEl(id: string, opts: { hidden?: boolean; text?: string } = {}): HTMLElement {
    const el = document.createElement("div");
    el.id = id;
    if (opts.hidden) {
      el.setAttribute("hidden", "hidden");
    }
    if (opts.text !== undefined) {
      el.textContent = opts.text;
    }
    document.body.appendChild(el);
    createdEls.push(el);
    return el;
  }

  afterEach(() => {
    lastFixture?.destroy();
    lastFixture = undefined;
    createdEls.forEach(el => el.remove());
    createdEls = [];
  });

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

    lastFixture = TestBed.createComponent(NzModalCommentBoxComponent);
    return lastFixture;
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

  describe("edit flows", () => {
    it("toggleEditInput is a no-op when its DOM targets are missing", async () => {
      const fixture = await createFixture({ user: makeUser() });
      const component = fixture.componentInstance;
      component.editValue = "preset";

      // No txarea/comment/editbtn elements exist for this name+time.
      component.toggleEditInput("Ghost", CREATION_TIME);

      expect(component.editValue).toBe("preset"); // early return leaves state untouched
    });

    it("toggleEditInput opens the editor, hides the comment, and loads its text", async () => {
      const fixture = await createFixture({ user: makeUser() });
      const component = fixture.componentInstance;
      const name = "Bob";
      const txarea = makeEl("txarea" + name + CREATION_TIME, { hidden: true });
      const comment = makeEl("comment" + name + CREATION_TIME, { text: "original text" });
      const btn = makeEl("editbtn" + name + CREATION_TIME, { hidden: true });

      component.toggleEditInput(name, CREATION_TIME);

      expect(txarea.hasAttribute("hidden")).toBe(false);
      expect(btn.hasAttribute("hidden")).toBe(false);
      expect(comment.hasAttribute("hidden")).toBe(true);
      expect(component.editValue).toBe("original text");
    });

    it("toggleEditInput closes the editor and clears editValue on the second toggle", async () => {
      const fixture = await createFixture({ user: makeUser() });
      const component = fixture.componentInstance;
      const name = "Bob";
      const txarea = makeEl("txarea" + name + CREATION_TIME, { hidden: true });
      const comment = makeEl("comment" + name + CREATION_TIME, { text: "original text" });
      const btn = makeEl("editbtn" + name + CREATION_TIME, { hidden: true });

      component.toggleEditInput(name, CREATION_TIME); // open
      component.toggleEditInput(name, CREATION_TIME); // close

      expect(txarea.hasAttribute("hidden")).toBe(true);
      expect(btn.hasAttribute("hidden")).toBe(true);
      expect(comment.hasAttribute("hidden")).toBe(false);
      expect(component.editValue).toBe("");
    });

    it("editComment is a no-op without a current user and preserves editValue", async () => {
      const fixture = await createFixture({ user: undefined });
      const component = fixture.componentInstance;
      component.editValue = "attempted edit";

      component.editComment(1, "Alice", CREATION_TIME);

      expect(editComment).not.toHaveBeenCalled();
      expect(component.editValue).toBe("attempted edit"); // reset happens only past the guard
    });

    it("editComment saves the new content and then hides the edit textarea and button", async () => {
      const fixture = await createFixture({ user: makeUser() });
      const component = fixture.componentInstance;
      const name = "Carol";
      const txarea = makeEl("txarea" + name + CREATION_TIME); // visible
      const btn = makeEl("editbtn" + name + CREATION_TIME); // visible
      component.editValue = "edited body";

      component.editComment(2, name, CREATION_TIME);

      expect(editComment).toHaveBeenCalledWith(2, CREATION_TIME, BOX_ID, "edited body");
      expect(component.editValue).toBe("");
      expect(txarea.hasAttribute("hidden")).toBe(true);
      expect(btn.hasAttribute("hidden")).toBe(true);
    });

    it("editComment still saves when the edit DOM targets are absent (guarded tail)", async () => {
      const fixture = await createFixture({ user: makeUser() });
      const component = fixture.componentInstance;
      component.editValue = "no-dom edit";

      // No txarea/editbtn elements: the service call happens, the DOM tail short-circuits.
      component.editComment(3, "Nobody", CREATION_TIME);

      expect(editComment).toHaveBeenCalledWith(3, CREATION_TIME, BOX_ID, "no-dom edit");
      expect(component.editValue).toBe("");
    });
  });

  it("the window:keydown host binding submits the comment on Ctrl+Enter", async () => {
    const fixture = await createFixture({ user: makeUser() });
    fixture.detectChanges(); // register the @HostListener("window:keydown") binding
    fixture.componentInstance.inputValue = "via host listener";

    const event = new KeyboardEvent("keydown", { key: "Enter", ctrlKey: true, cancelable: true });
    window.dispatchEvent(event);

    expect(addComment).toHaveBeenCalledTimes(1);
    expect(addComment.mock.calls[0][0]).toMatchObject({ content: "via host listener" });
    expect(event.defaultPrevented).toBe(true);
  });

  // ─── template wiring ──────────────────────────────────────────────────────
  // Every test above reaches the component's methods directly, so none of the
  // template's own event bindings ever run. These drive the rendered DOM.
  describe("template wiring", () => {
    // The comment's author is deliberately NOT the signed-in user (makeUser is
    // uid 1 / "Alice"). If the two shared their id and name, a binding that
    // handed a handler `user.uid` where the template must hand it
    // `item['creatorID']` — the classic "whose comment is this" wiring bug —
    // would be indistinguishable from the correct one.
    const COMMENT = { content: "original", creatorName: "Bob", creatorID: 7, creationTime: CREATION_TIME };

    async function renderWithOneComment(): Promise<ComponentFixture<NzModalCommentBoxComponent>> {
      const fixture = await createFixture({ user: makeUser(), comments: [COMMENT] });
      fixture.detectChanges();
      return fixture;
    }

    function actionLinks(fixture: ComponentFixture<NzModalCommentBoxComponent>): HTMLAnchorElement[] {
      return Array.from(fixture.nativeElement.querySelectorAll("[nz-list-item-actions] a")) as HTMLAnchorElement[];
    }

    function footerTextarea(fixture: ComponentFixture<NzModalCommentBoxComponent>): HTMLTextAreaElement {
      return fixture.nativeElement.querySelector(".modal-footer textarea") as HTMLTextAreaElement;
    }

    function footerButton(fixture: ComponentFixture<NzModalCommentBoxComponent>): HTMLButtonElement {
      return fixture.nativeElement.querySelector(".modal-footer button") as HTMLButtonElement;
    }

    // The per-comment ids embed an ISO timestamp, whose colons and dots are not
    // legal in a CSS `#id` selector, so match on the attribute instead.
    function byId<T extends HTMLElement>(fixture: ComponentFixture<NzModalCommentBoxComponent>, id: string): T {
      return fixture.nativeElement.querySelector(`[id="${id}"]`) as T;
    }

    it("posts the typed comment when the footer send button is pressed", async () => {
      const fixture = await renderWithOneComment();
      const component = fixture.componentInstance;
      component.inputValue = "from the button";
      // The button carries [disabled]="!user || !inputValue"; jsdom swallows a
      // click on a disabled button, so the model has to be flushed first.
      fixture.detectChanges();

      const button = footerButton(fixture);
      expect(button.disabled).toBe(false);
      button.click();

      expect(addComment).toHaveBeenCalledTimes(1);
      expect(addComment.mock.calls[0][0]).toMatchObject({ content: "from the button" });
      // onClickAddComment (not addComment) is what clears the box.
      expect(component.inputValue).toBe("");
    });

    it("keeps the footer send button disabled while the box is empty", async () => {
      const fixture = await renderWithOneComment();

      expect(footerButton(fixture).disabled).toBe(true);
    });

    it("keeps both send buttons disabled while nobody is signed in", async () => {
      // Everything else in this describe renders with a signed-in user, so the
      // `!user` operand of both [disabled] expressions is otherwise never seen
      // from its true side. Without it a signed-out viewer gets an enabled Send
      // whose click is silently dropped by addComment/editComment's own guard.
      const fixture = await createFixture({ user: undefined, comments: [COMMENT] });
      const component = fixture.componentInstance;
      component.inputValue = "typed while signed out";
      component.editValue = "edited while signed out";
      fixture.detectChanges();

      expect(footerButton(fixture).disabled).toBe(true);
      expect(byId<HTMLButtonElement>(fixture, `editbtn${COMMENT.creatorName}${CREATION_TIME}`).disabled).toBe(true);
    });

    it("writes the footer textarea back into inputValue through ngModel", async () => {
      const fixture = await renderWithOneComment();
      const textarea = footerTextarea(fixture);

      textarea.value = "typed into the box";
      textarea.dispatchEvent(new Event("input"));

      expect(fixture.componentInstance.inputValue).toBe("typed into the box");
      expect(fixture.componentInstance.editValue).toBe("");
    });

    it("submits the footer comment on Enter", async () => {
      const fixture = await renderWithOneComment();
      const component = fixture.componentInstance;
      component.inputValue = "sent with the enter key";
      fixture.detectChanges();

      const event = new KeyboardEvent("keydown", { key: "Enter", cancelable: true, bubbles: true });
      footerTextarea(fixture).dispatchEvent(event);

      expect(addComment).toHaveBeenCalledTimes(1);
      expect(addComment.mock.calls[0][0]).toMatchObject({ content: "sent with the enter key" });
      // Enter goes through onClickAddComment, so the box is emptied too.
      expect(component.inputValue).toBe("");
      // Otherwise Enter would also insert a newline in the box it just cleared.
      expect(event.defaultPrevented).toBe(true);
    });

    it("does not submit the footer comment on an ordinary keystroke", async () => {
      // The binding is (keydown.enter); with the key filter dropped, every
      // character typed would post the comment and preventDefault the keystroke,
      // making the box impossible to type in.
      const fixture = await renderWithOneComment();
      const component = fixture.componentInstance;
      component.inputValue = "half typed";
      fixture.detectChanges();

      const event = new KeyboardEvent("keydown", { key: "a", cancelable: true, bubbles: true });
      footerTextarea(fixture).dispatchEvent(event);

      expect(addComment).not.toHaveBeenCalled();
      expect(component.inputValue).toBe("half typed");
      expect(event.defaultPrevented).toBe(false);
    });

    it("saves the edited body when the per-comment send button is pressed", async () => {
      const fixture = await renderWithOneComment();
      const component = fixture.componentInstance;
      const button = byId<HTMLButtonElement>(fixture, `editbtn${COMMENT.creatorName}${CREATION_TIME}`);
      const textarea = byId<HTMLTextAreaElement>(fixture, `txarea${COMMENT.creatorName}${CREATION_TIME}`);
      // [disabled]="!user || !editValue": an empty edit box cannot be sent.
      expect(button.disabled).toBe(true);

      // Open the editor the way the UI does rather than assigning editValue, so
      // the textarea and button are really revealed and the body is loaded.
      actionLinks(fixture)[1].click();
      fixture.detectChanges();
      expect(textarea.hasAttribute("hidden")).toBe(false);
      expect(button.hasAttribute("hidden")).toBe(false);
      expect(component.editValue).toBe("original");
      expect(button.disabled).toBe(false);

      button.click();

      expect(editComment).toHaveBeenCalledWith(COMMENT.creatorID, CREATION_TIME, BOX_ID, "original");
      expect(deleteComment).not.toHaveBeenCalled();
      // editComment's DOM tail closes the editor again, and it can only find
      // those two nodes through the creatorName the template hands it.
      expect(textarea.hasAttribute("hidden")).toBe(true);
      expect(button.hasAttribute("hidden")).toBe(true);
    });

    it("writes the per-comment textarea back into editValue through ngModel", async () => {
      const fixture = await renderWithOneComment();
      const textarea = byId<HTMLTextAreaElement>(fixture, `txarea${COMMENT.creatorName}${CREATION_TIME}`);

      textarea.value = "an edit in progress";
      textarea.dispatchEvent(new Event("input"));

      expect(fixture.componentInstance.editValue).toBe("an edit in progress");
      expect(fixture.componentInstance.inputValue).toBe("");
    });

    it("saves the edited body on Enter in the per-comment textarea", async () => {
      const fixture = await renderWithOneComment();
      const textarea = byId<HTMLTextAreaElement>(fixture, `txarea${COMMENT.creatorName}${CREATION_TIME}`);
      const button = byId<HTMLButtonElement>(fixture, `editbtn${COMMENT.creatorName}${CREATION_TIME}`);
      // Open the editor through the real link, so editComment's DOM tail has an
      // editor to close -- that tail is the only thing the creatorName argument
      // this binding passes can be observed through.
      actionLinks(fixture)[1].click();
      fixture.componentInstance.editValue = "edited with the enter key";
      fixture.detectChanges();
      expect(textarea.hasAttribute("hidden")).toBe(false);

      const event = new KeyboardEvent("keydown", { key: "Enter", cancelable: true, bubbles: true });
      textarea.dispatchEvent(event);

      expect(editComment).toHaveBeenCalledWith(COMMENT.creatorID, CREATION_TIME, BOX_ID, "edited with the enter key");
      expect(event.defaultPrevented).toBe(true);
      expect(textarea.hasAttribute("hidden")).toBe(true);
      expect(button.hasAttribute("hidden")).toBe(true);
    });

    it("does not save the edit on an ordinary keystroke in the per-comment textarea", async () => {
      // Same key filter, same failure mode: without (keydown.enter) every
      // character typed into the edit box would commit the edit.
      const fixture = await renderWithOneComment();
      fixture.componentInstance.editValue = "half edited";
      fixture.detectChanges();

      const textarea = byId<HTMLTextAreaElement>(fixture, `txarea${COMMENT.creatorName}${CREATION_TIME}`);
      const event = new KeyboardEvent("keydown", { key: "a", cancelable: true, bubbles: true });
      textarea.dispatchEvent(event);

      expect(editComment).not.toHaveBeenCalled();
      expect(fixture.componentInstance.editValue).toBe("half edited");
      expect(event.defaultPrevented).toBe(false);
    });

    it("wires the delete, edit and reply action links to their own handlers", async () => {
      const fixture = await renderWithOneComment();
      const component = fixture.componentInstance;
      const [deleteLink, editLink, replyLink] = actionLinks(fixture);
      expect([deleteLink.textContent, editLink.textContent, replyLink.textContent]).toEqual([
        "delete",
        "edit",
        "reply",
      ]);

      replyLink.click();
      // The author, not the viewer: a reply quotes "Bob", never "Alice".
      expect(component.inputValue).toBe('@Bob:"original"\n');

      // toggleEditInput needs the ids the template itself renders.
      editLink.click();
      expect(component.editValue).toBe("original");

      deleteLink.click();
      expect(deleteComment).toHaveBeenCalledWith(COMMENT.creatorID, CREATION_TIME, BOX_ID);
    });
  });
});
