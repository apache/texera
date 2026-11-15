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
import * as Y from "yjs";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { OperatorDescriptionComponent } from "./operator-description.component";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowGraph } from "../../../service/workflow-graph/model/workflow-graph";
import { WorkflowUtilService } from "../../../service/workflow-graph/util/workflow-util.service";
import { JointUIService } from "../../../service/joint-ui/joint-ui.service";
import { UndoRedoService } from "../../../service/undo-redo/undo-redo.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import {
  mockPoint,
  mockScanPredicate,
  mockSentimentPredicate,
} from "../../../service/workflow-graph/model/mock-workflow-data";
import { MAX_OPERATOR_DESCRIPTION_LENGTH } from "../../../util/operator-description.util";
import { OperatorPredicate } from "../../../types/workflow-common.interface";

describe("OperatorDescriptionComponent", () => {
  let fixture: ComponentFixture<OperatorDescriptionComponent>;
  let component: OperatorDescriptionComponent;
  let workflowActionService: WorkflowActionService;
  const scanID = mockScanPredicate.operatorID;

  const el = (): HTMLElement => fixture.nativeElement;
  const q = (selector: string) => el().querySelector(selector) as HTMLElement | null;
  const textarea = () => q("textarea") as HTMLTextAreaElement;
  const description = (id = scanID) => workflowActionService.getTexeraGraph().getOperator(id).description;
  const view = () => q(".instance-description-view");

  function setOperator(operatorId: string, editable = true) {
    fixture.componentRef.setInput("operatorId", operatorId);
    fixture.componentRef.setInput("editable", editable);
    fixture.detectChanges();
  }

  function setEditable(editable: boolean) {
    fixture.componentRef.setInput("editable", editable);
    fixture.detectChanges();
  }

  async function startEditing(text?: string) {
    q(".instance-description-edit")!.click();
    fixture.detectChanges();
    await fixture.whenStable(); // let ngModel write the initial draft before typing over it
    if (text !== undefined) {
      textarea().value = text;
      textarea().dispatchEvent(new Event("input"));
      fixture.detectChanges();
    }
  }

  function key(init: KeyboardEventInit): KeyboardEvent {
    const event = new KeyboardEvent("keydown", { bubbles: true, cancelable: true, ...init });
    textarea().dispatchEvent(event);
    fixture.detectChanges();
    return event;
  }

  /** Re-creates an operator under the same ID, as a reload or version preview does. */
  function recreateOperator(operator: OperatorPredicate) {
    workflowActionService.deleteOperator(operator.operatorID);
    workflowActionService.addOperator(operator, mockPoint);
  }

  /**
   * A co-editor saving from their own document, as in a shared session: their change reaches this editor
   * as a Yjs update, not as a call on this editor's services.
   */
  function coEditorSaves(text: string, operatorId = scanID) {
    const localDoc = (workflowActionService.getTexeraGraph() as WorkflowGraph).sharedModel.yDoc;
    const remote = new Y.Doc();
    Y.applyUpdate(remote, Y.encodeStateAsUpdate(localDoc));
    const remoteText = (remote.getMap("operatorIDMap").get(operatorId) as Y.Map<unknown>).get("description") as Y.Text;
    remote.transact(() => {
      remoteText.delete(0, remoteText.length);
      remoteText.insert(0, text);
    });
    Y.applyUpdate(localDoc, Y.encodeStateAsUpdate(remote, Y.encodeStateVector(localDoc)));
    remote.destroy();
    fixture.detectChanges();
  }

  beforeEach(() => {
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    TestBed.configureTestingModule({
      imports: [OperatorDescriptionComponent, NoopAnimationsModule],
      providers: [
        WorkflowActionService,
        WorkflowUtilService,
        JointUIService,
        UndoRedoService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        ...commonTestProviders,
      ],
    });
    workflowActionService = TestBed.inject(WorkflowActionService);
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    fixture = TestBed.createComponent(OperatorDescriptionComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    workflowActionService.getTexeraGraph().destroyYModel();
    vi.restoreAllMocks();
  });

  describe("display", () => {
    it("shows an add-description prompt when the operator has no description", () => {
      setOperator(scanID);
      expect(q(".instance-description-empty")?.textContent).toContain("Add a description");
      expect(view()).toBeNull();
    });

    it("renders an existing description as markdown", () => {
      workflowActionService.setOperatorDescription(scanID, "Keeps **only** US rows\n\n- one\n- two");
      setOperator(scanID);
      expect(view()?.querySelector("strong")?.textContent).toEqual("only");
      expect(view()?.querySelectorAll("li").length).toBe(2);
    });

    it("hides edit affordances when not editable, but still shows the description", () => {
      workflowActionService.setOperatorDescription(scanID, "read only note");
      setOperator(scanID, false);
      expect(q(".instance-description-edit")).toBeNull();
      expect(view()?.textContent).toContain("read only note");
    });

    it("renders nothing for a non-editable operator without a description", () => {
      setOperator(scanID, false);
      expect(q(".instance-description")).toBeNull();
    });

    it("shows the add prompt, without throwing, when the stored description is not text", () => {
      recreateOperator({ ...mockScanPredicate, description: 5 as any });
      expect(() => setOperator(scanID)).not.toThrow();
      expect(component.description).toEqual("");
      expect(q(".instance-description-empty")).toBeTruthy();
    });

    it("does nothing for an operator ID that does not exist", () => {
      setOperator("ghost-operator");
      expect(component.description).toEqual("");
      expect(q(".instance-description-empty")).toBeTruthy();
    });

    it("follows a co-editor's change while not editing", () => {
      setOperator(scanID);
      workflowActionService.setOperatorDescription(scanID, "from elsewhere");
      fixture.detectChanges();
      expect(view()?.textContent).toContain("from elsewhere");
    });

    it("ignores description changes on other operators", () => {
      workflowActionService.addOperator(mockSentimentPredicate, { x: 300, y: 300 });
      setOperator(scanID);
      workflowActionService.setOperatorDescription(mockSentimentPredicate.operatorID, "other op");
      expect(component.description).toEqual("");
    });
  });

  describe("untrusted markdown", () => {
    function render(markdown: string): HTMLElement {
      workflowActionService.setOperatorDescription(scanID, markdown);
      setOperator(scanID);
      return view()!;
    }

    it("shows raw HTML as text instead of creating elements", () => {
      const rendered = render('<div class="ant-modal-wrap"><a href="https://phish.example">Sign in</a></div>');
      expect(rendered.querySelector(".ant-modal-wrap")).toBeNull();
      expect(rendered.querySelector("div")).toBeNull();
      expect(rendered.textContent).toContain('<div class="ant-modal-wrap">');
    });

    it("removes scripts, event handlers, iframes and svg", () => {
      const rendered = render(
        'hi <img src="x" onerror="alert(1)"><script>alert(2)</script><iframe src="https://x"></iframe><svg onload="alert(3)"></svg>'
      );
      for (const tag of ["script", "img", "iframe", "svg"]) expect(rendered.querySelector(tag)).toBeNull();
      expect(rendered.innerHTML).not.toMatch(/<[^>]+\son\w+=/i);
    });

    it("turns markdown images into links so no remote image is fetched", () => {
      const rendered = render("![pixel](https://tracker.example/p.png?u=1)");
      expect(rendered.querySelector("img")).toBeNull();
      const link = rendered.querySelector("a")!;
      expect(link.getAttribute("href")).toEqual("https://tracker.example/p.png?u=1");
      expect(link.textContent).toContain("pixel");
    });

    it("drops javascript:, data: and other unsafe link targets but keeps the text", () => {
      const rendered = render(
        "[a](javascript:alert(1)) [b](data:text/html,<b>x</b>) [c](vbscript:x) [d](JAVASCRIPT:alert(1))"
      );
      expect(rendered.querySelector("a")).toBeNull();
      expect(rendered.textContent).toContain("a");
      expect(rendered.textContent).toContain("d");
    });

    it("opens safe links in a new tab without a referrer", () => {
      const rendered = render("[docs](https://texera.example/docs) and <mailto:me@example.com>");
      const links = Array.from(rendered.querySelectorAll("a"));
      expect(links.map(a => a.getAttribute("href"))).toEqual(["https://texera.example/docs", "mailto:me@example.com"]);
      for (const a of links) {
        expect(a.getAttribute("target")).toEqual("_blank");
        expect(a.getAttribute("rel")).toContain("noopener");
        expect(a.getAttribute("rel")).toContain("noreferrer");
      }
    });
  });

  describe("editing", () => {
    it("saves an edited description to the operator", async () => {
      setOperator(scanID);
      await startEditing("## Why\nDrops rows with null ids");
      q(".instance-description-save")!.click();
      fixture.detectChanges();

      expect(description()).toEqual("## Why\nDrops rows with null ids");
      expect(component.editing).toBe(false);
      expect(textarea()).toBeNull();
    });

    it("starts editing from the current description", async () => {
      workflowActionService.setOperatorDescription(scanID, "existing");
      setOperator(scanID);
      await startEditing();
      expect(textarea().value).toEqual("existing");
    });

    it("discards the draft on cancel", async () => {
      workflowActionService.setOperatorDescription(scanID, "keep me");
      setOperator(scanID);
      await startEditing("throw me away");
      q(".instance-description-cancel")!.click();
      fixture.detectChanges();

      expect(description()).toEqual("keep me");
      expect(component.editing).toBe(false);
    });

    it("saves on Cmd+Enter and on Ctrl+Enter", async () => {
      setOperator(scanID);
      await startEditing("via cmd");
      key({ key: "Enter", metaKey: true });
      expect(description()).toEqual("via cmd");

      await startEditing("via ctrl");
      key({ key: "Enter", ctrlKey: true });
      expect(description()).toEqual("via ctrl");
    });

    it("cancels on Escape without letting the key reach the rest of the page", async () => {
      setOperator(scanID);
      await startEditing("not saved");
      const outside = vi.fn();
      document.addEventListener("keydown", outside);
      const event = key({ key: "Escape" });
      document.removeEventListener("keydown", outside);

      expect(outside).not.toHaveBeenCalled();
      expect(event.defaultPrevented).toBe(true);
      expect(description()).toBeUndefined();
      expect(component.editing).toBe(false);
    });

    it("keeps Cmd/Ctrl+Z and Cmd/Ctrl+Y inside the text box so the canvas does not also undo", async () => {
      setOperator(scanID);
      await startEditing("typing");
      const outside = vi.fn();
      document.addEventListener("keydown", outside);
      key({ key: "z", metaKey: true });
      key({ key: "Z", ctrlKey: true, shiftKey: true });
      key({ key: "y", ctrlKey: true });
      document.removeEventListener("keydown", outside);

      expect(outside).not.toHaveBeenCalled();
      expect(component.editing).toBe(true);
    });

    it("does not save on a plain Enter, so multi-line notes can be typed", async () => {
      setOperator(scanID);
      await startEditing("line one");
      key({ key: "Enter" });
      expect(component.editing).toBe(true);
      expect(description()).toBeUndefined();
    });

    it("clears the description when a whitespace-only draft is saved", async () => {
      workflowActionService.setOperatorDescription(scanID, "something");
      setOperator(scanID);
      await startEditing("   \n\t ");
      q(".instance-description-save")!.click();
      fixture.detectChanges();
      expect(description()).toBeUndefined();
      expect(q(".instance-description-empty")).toBeTruthy();
    });

    it("does not write when the draft is unchanged", async () => {
      workflowActionService.setOperatorDescription(scanID, "same");
      setOperator(scanID);
      const spy = vi.spyOn(workflowActionService, "setOperatorDescription");
      await startEditing();
      q(".instance-description-save")!.click();
      expect(spy).not.toHaveBeenCalled();
    });

    it("limits the text box to the maximum description length and shows a counter", async () => {
      setOperator(scanID);
      await startEditing("abc");
      expect(textarea().getAttribute("maxlength")).toEqual(String(MAX_OPERATOR_DESCRIPTION_LENGTH));
      expect(q(".instance-description-count")?.textContent).toContain(`3 / ${MAX_OPERATOR_DESCRIPTION_LENGTH}`);
    });

    it("tells the user the note is visible to collaborators and AI agents", async () => {
      setOperator(scanID);
      await startEditing();
      expect(q(".instance-description-hint")?.textContent).toContain("Visible to collaborators and AI agents");
    });

    it("ignores startEditing when not editable", () => {
      setOperator(scanID, false);
      component.startEditing();
      expect(component.editing).toBe(false);
    });
  });

  describe("co-editors", () => {
    it("keeps the local draft and flags the conflict when a co-editor saves mid-edit", async () => {
      setOperator(scanID);
      await startEditing("my draft");
      workflowActionService.setOperatorDescription(scanID, "their change");
      fixture.detectChanges();
      await fixture.whenStable();

      expect(component.draft).toEqual("my draft");
      expect(textarea().value).toEqual("my draft");
      expect(q(".instance-description-conflict")?.textContent).toContain("Someone else changed this description");
    });

    it("loads the co-editor's version on request", async () => {
      setOperator(scanID);
      await startEditing("my draft");
      workflowActionService.setOperatorDescription(scanID, "their change");
      fixture.detectChanges();
      q(".instance-description-load-latest")!.click();
      fixture.detectChanges();

      expect(component.draft).toEqual("their change");
      expect(q(".instance-description-conflict")).toBeNull();
    });

    it("does not flag a conflict for the user's own save", async () => {
      setOperator(scanID);
      await startEditing("mine");
      q(".instance-description-save")!.click();
      fixture.detectChanges();
      await startEditing("mine again");
      expect(component.changedElsewhere).toBe(false);
    });
  });

  describe("drafts at the edges of an edit", () => {
    it("saves a pending draft to the previous operator when switching operators mid-edit", async () => {
      workflowActionService.addOperator(mockSentimentPredicate, { x: 300, y: 300 });
      setOperator(scanID);
      await startEditing("unsaved scan note");

      setOperator(mockSentimentPredicate.operatorID);

      expect(description(scanID)).toEqual("unsaved scan note");
      expect(component.editing).toBe(false);
      expect(component.description).toEqual("");
    });

    it("saves a pending draft when the panel is destroyed mid-edit", async () => {
      setOperator(scanID);
      await startEditing("closing the panel");
      fixture.destroy();
      expect(description()).toEqual("closing the panel");
    });

    it("keeps the draft but writes nothing when editing gets locked mid-edit", async () => {
      workflowActionService.setOperatorDescription(scanID, "before lock");
      setOperator(scanID);
      await startEditing("typed before the lock");
      const spy = vi.spyOn(workflowActionService, "setOperatorDescription");

      setEditable(false);
      await fixture.whenStable();

      expect(component.editing).toBe(true);
      expect(textarea().disabled).toBe(true);
      expect((q(".instance-description-save") as HTMLButtonElement).disabled).toBe(true);
      expect(q(".instance-description-hint")?.textContent).toContain("locked");
      component.save();
      expect(spy).not.toHaveBeenCalled();
      expect(description()).toEqual("before lock");

      setEditable(true);
      q(".instance-description-save")!.click();
      fixture.detectChanges();
      expect(description()).toEqual("typed before the lock");
    });

    it("does not write when the panel is destroyed while locked", async () => {
      setOperator(scanID);
      await startEditing("draft");
      setEditable(false);
      fixture.destroy();
      expect(description()).toBeUndefined();
    });

    it("does not write a draft into an operator re-created under the same ID (reload, version preview)", async () => {
      setOperator(scanID);
      await startEditing("my unsaved draft");

      recreateOperator({ ...mockScanPredicate, description: "historic version" });
      fixture.destroy();

      expect(description()).toEqual("historic version");
    });

    describe("an editor opened but not typed in, while a co-editor saves", () => {
      beforeEach(async () => {
        workflowActionService.setOperatorDescription(scanID, "original");
        setOperator(scanID);
        await startEditing(); // opened, never typed in
        coEditorSaves("new note");
      });

      it("keeps the co-editor's text when the panel closes", () => {
        fixture.destroy();
        expect(description()).toEqual("new note");
      });

      it("keeps the co-editor's text when switching to another operator", () => {
        workflowActionService.addOperator(mockSentimentPredicate, { x: 300, y: 300 });
        setOperator(mockSentimentPredicate.operatorID);
        expect(description()).toEqual("new note");
      });

      it("still replaces it when the user explicitly saves, as the conflict banner says", () => {
        q(".instance-description-save")!.click();
        fixture.detectChanges();
        expect(description()).toEqual("original");
      });
    });

    it("keeps a co-editor's text when an untouched editor closes on an operator that had no description", async () => {
      setOperator(scanID);
      await startEditing();
      coEditorSaves("their first note");
      fixture.destroy();
      expect(description()).toEqual("their first note");
    });

    it("still saves a typed draft over a co-editor's change when leaving the editor", async () => {
      setOperator(scanID);
      await startEditing("my edit");
      coEditorSaves("their edit");
      fixture.destroy();
      expect(description()).toEqual("my edit");
    });

    it("does not throw or write when the operator is deleted mid-edit", async () => {
      setOperator(scanID);
      await startEditing("orphan draft");
      const spy = vi.spyOn(workflowActionService, "setOperatorDescription");
      workflowActionService.deleteOperator(scanID);

      expect(() => component.save()).not.toThrow();
      expect(() => fixture.destroy()).not.toThrow();
      expect(spy).not.toHaveBeenCalled();
    });
  });
});
