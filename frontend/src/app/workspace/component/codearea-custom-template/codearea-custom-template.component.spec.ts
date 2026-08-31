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
import { CodeareaCustomTemplateComponent } from "./codearea-custom-template.component";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { FormControl } from "@angular/forms";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { CodeEditorService } from "../../service/code-editor/code-editor.service";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { config as rxjsConfig, Subject, take } from "rxjs";

describe("CodeareaCustomTemplateComponent", () => {
  let component: CodeareaCustomTemplateComponent;
  let fixture: ComponentFixture<CodeareaCustomTemplateComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CodeareaCustomTemplateComponent, HttpClientTestingModule],
      providers: [
        WorkflowActionService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(CodeareaCustomTemplateComponent);
    component = fixture.componentInstance;
    component.field = { props: {}, formControl: new FormControl() };
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  /**
   * The component's whole job is to keep one shared, per-operator "is the editor open" flag in
   * sync across three inputs: this component's own open/close, the same operator's editor opened
   * by a co-editor, and the component being torn down while the editor is still up. The flag lives
   * in CodeEditorService keyed by operator id, so a mix-up there silently reopens (or fails to
   * reopen) the wrong operator's editor.
   *
   * `openEditor` is driven directly rather than through the template button: it calls
   * `codeEditorService.vc.createComponent`, and `vc` is a ViewContainerRef the real workspace
   * supplies. The tests below install a fake one so the component's own bookkeeping is observable
   * without standing up a Monaco editor.
   */
  describe("editor-open state", () => {
    let codeEditorService: CodeEditorService;
    let destroyCallbacks: (() => void)[];

    // The highlighted operator is what getOperatorID reads, and it is the key every
    // setEditorState/getEditorState call is scoped by.
    function highlightedOperatorId(): string {
      return TestBed.inject(WorkflowActionService).getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
    }

    // The shared flag as the service currently publishes it for one operator. `getEditorState`
    // hands back a BehaviorSubject as an observable, so the current value arrives synchronously on
    // subscribe; `take(1)` consumes that one value and completes, instead of leaving a live
    // subscription on a service-owned subject behind for the rest of the test. Returns
    // `boolean | undefined` on purpose: a service that stopped replaying a current value would
    // yield `undefined` here and fail the caller's assertion rather than quietly reading stale.
    function publishedEditorState(operatorId: string): boolean | undefined {
      let published: boolean | undefined;
      codeEditorService
        .getEditorState(operatorId)
        .pipe(take(1))
        .subscribe(v => (published = v));
      return published;
    }

    beforeEach(() => {
      const workflowActionService = TestBed.inject(WorkflowActionService);
      vi.spyOn(workflowActionService.getJointGraphWrapper(), "getCurrentHighlightedOperatorIDs").mockReturnValue([
        "test-operator-id",
      ]);
      // ngOnInit reads the highlighted operator id; re-run it so operatorID is deterministic for these tests.
      component.ngOnInit();

      codeEditorService = TestBed.inject(CodeEditorService);
      destroyCallbacks = [];
      // Stand-in for the workspace's ViewContainerRef. Records the onDestroy hook the component
      // registers so the close path can be triggered without a real component teardown.
      codeEditorService.vc = {
        createComponent: () => ({
          instance: {} as any,
          onDestroy: (cb: () => void) => destroyCallbacks.push(cb),
          destroy: () => destroyCallbacks.forEach(cb => cb()),
        }),
      } as any;
    });

    it("marks the operator's editor open and hands the field's control to it", () => {
      component.openEditor();

      expect(component.isEditorOpen).toBe(true);
      // The created editor must edit THIS field, not a fresh control - otherwise the dialog opens
      // detached from the operator property it is meant to edit.
      expect(component.componentRef!.instance.formControl).toBe(component.field.formControl);

      expect(publishedEditorState(highlightedOperatorId())).toBe(true);
    });

    it("clears the flag again when the editor component is destroyed", () => {
      component.openEditor();
      component.componentRef!.destroy();

      expect(component.isEditorOpen).toBe(false);
      expect(publishedEditorState(highlightedOperatorId())).toBe(false);
    });

    it("opens the editor when a co-editor opens one", () => {
      // The constructor subscribes to the co-editor stream, so the stub has to be in place before
      // the component exists - hence a fresh fixture here rather than the shared one. Stubbing the
      // getter (rather than casting its result back to a Subject) keeps this honest: it returns
      // `asObservable()`, so a cast would only work by accident of the current implementation.
      const opened = new Subject<{ operatorId: string }>();
      vi.spyOn(TestBed.inject(CoeditorPresenceService), "getCoeditorOpenedCodeEditorSubject").mockReturnValue(
        opened.asObservable()
      );

      const remoteFixture = TestBed.createComponent(CodeareaCustomTemplateComponent);
      const remoteComponent = remoteFixture.componentInstance;
      remoteComponent.field = { props: {}, formControl: new FormControl() } as any;
      remoteFixture.detectChanges();

      expect(remoteComponent.isEditorOpen).toBe(false);

      // A remote open has to bring this client's editor up too - that is what makes the session
      // collaborative rather than merely presence-aware.
      opened.next({ operatorId: highlightedOperatorId() });

      expect(remoteComponent.isEditorOpen).toBe(true);
      expect(remoteComponent.componentRef).toBeDefined();
    });

    it("tears this client's editor down when any co-editor closes one, whichever operator it names", () => {
      // The mirror image of the open case above, and the half that was missing: a co-editor
      // closing the dialog has to close it here too, or this client keeps typing into an editor
      // the other side has already dismissed.
      const closed = new Subject<{ operatorId: string }>();
      vi.spyOn(TestBed.inject(CoeditorPresenceService), "getCoeditorClosedCodeEditorSubject").mockReturnValue(
        closed.asObservable()
      );

      const remoteFixture = TestBed.createComponent(CodeareaCustomTemplateComponent);
      const remoteComponent = remoteFixture.componentInstance;
      remoteComponent.field = { props: {}, formControl: new FormControl() } as any;
      remoteFixture.detectChanges();
      remoteComponent.openEditor();
      expect(remoteComponent.isEditorOpen).toBe(true);

      // The subscriber binds the payload to `_` and discards it, so a deliberately foreign operator
      // id is the honest fixture: naming this panel's own operator would advertise a targeting
      // filter the component does not have, and would leave a later filter free to be added without
      // any test noticing.
      const destroySpy = vi.spyOn(remoteComponent.componentRef!, "destroy");
      closed.next({ operatorId: "a-completely-unrelated-operator" });

      // The spy is what proves teardown. The shared flag below is reachable *without* any teardown:
      // a subscriber that merely published `false` for this operator would satisfy it through
      // ngOnInit's getEditorState subscription while the dialog stayed on screen. vi.spyOn calls
      // through, so the real destroy still runs and the flag assertions still describe the result.
      expect(destroySpy).toHaveBeenCalledTimes(1);
      expect(remoteComponent.isEditorOpen).toBe(false);
      expect(publishedEditorState(highlightedOperatorId())).toBe(false);
    });

    it("stays quiet when a co-editor closes an editor this panel never opened", async () => {
      // componentRef is still undefined here; the optional call is what keeps a close broadcast
      // from throwing in every panel that happens to be mounted but closed. A close broadcast
      // reaches EVERY mounted codearea, so most recipients are in exactly this state.
      const closed = new Subject<{ operatorId: string }>();
      vi.spyOn(TestBed.inject(CoeditorPresenceService), "getCoeditorClosedCodeEditorSubject").mockReturnValue(
        closed.asObservable()
      );

      const remoteFixture = TestBed.createComponent(CodeareaCustomTemplateComponent);
      const remoteComponent = remoteFixture.componentInstance;
      remoteComponent.field = { props: {}, formControl: new FormControl() } as any;
      remoteFixture.detectChanges();

      // A throw inside a subscriber does NOT propagate out of Subject.next() - RxJS swallows it and
      // reports it on its unhandled-error channel one macrotask later. So `expect(...).not.toThrow()`
      // would pass even with the optional call removed; watch that channel instead.
      const unhandled = vi.fn();
      const previousHandler = rxjsConfig.onUnhandledError;
      // Two restores, for two different exits. The `finally` below is the normal one, and it puts
      // the handler back before the assertions so anything RxJS reports afterwards is reported,
      // not swallowed. It is skipped, though, if the awaited flush never resolves and Vitest kills
      // this test on its 20s timeout - and every spec in this FILE shares one forked worker, so a
      // handler left installed would silently eat any genuine unhandled error the remaining seven
      // raise. `onTestFinished` runs however the test ends, so it closes that exit; re-assigning
      // the same value after the `finally` already ran is a no-op. Deliberately not hoisted into
      // beforeEach/afterEach: that would install this swallowing handler for all eight tests.
      onTestFinished(() => {
        rxjsConfig.onUnhandledError = previousHandler;
      });
      rxjsConfig.onUnhandledError = unhandled;
      try {
        closed.next({ operatorId: "a-completely-unrelated-operator" });
        // A real macrotask, not fake timers. `next()` schedules RxJS's report timer synchronously,
        // before this line creates its own, and Node fires equal-delay timers in insertion order -
        // so the report always lands before the await resumes. Fake timers would buy nothing here
        // (~1ms of a 167ms file) and would cost the flush being a whole-timer-API swap while an
        // Angular fixture is live, draining whatever else the fixture had queued.
        await new Promise(resolve => setTimeout(resolve, 0));
      } finally {
        rxjsConfig.onUnhandledError = previousHandler;
      }

      expect(unhandled).not.toHaveBeenCalled();
      // Trivially satisfied against pristine production - nothing in this test can set the flag -
      // but not decorative: this is the assertion that catches the open and close subscriber bodies
      // being swapped, in which case a close broadcast would OPEN an editor in a panel that had
      // none. Measured: with this line the swap fails 3 tests here, without it only 2.
      expect(remoteComponent.isEditorOpen).toBe(false);
    });

    it("persists the open flag on destroy so a reopened panel restores it", () => {
      component.openEditor();

      component.ngOnDestroy();

      // ngOnDestroy writes the CURRENT flag rather than a hardcoded false, so a component torn
      // down with its editor still up comes back open.
      expect(publishedEditorState(highlightedOperatorId())).toBe(true);
    });

    it("tracks an external state change through ngOnInit's subscription", () => {
      // Another component sharing the same operator id flips the flag; this one must follow.
      codeEditorService.setEditorState(highlightedOperatorId(), true);
      expect(component.isEditorOpen).toBe(true);

      codeEditorService.setEditorState(highlightedOperatorId(), false);
      expect(component.isEditorOpen).toBe(false);
    });
  });
});
