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
import { Subject } from "rxjs";

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

      let published: boolean | undefined;
      codeEditorService.getEditorState(highlightedOperatorId()).subscribe(v => (published = v));
      expect(published).toBe(true);
    });

    it("clears the flag again when the editor component is destroyed", () => {
      component.openEditor();
      component.componentRef!.destroy();

      expect(component.isEditorOpen).toBe(false);
      let published: boolean | undefined;
      codeEditorService.getEditorState(highlightedOperatorId()).subscribe(v => (published = v));
      expect(published).toBe(false);
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

    it("persists the open flag on destroy so a reopened panel restores it", () => {
      component.openEditor();

      component.ngOnDestroy();

      // ngOnDestroy writes the CURRENT flag rather than a hardcoded false, so a component torn
      // down with its editor still up comes back open.
      let published: boolean | undefined;
      codeEditorService.getEditorState(highlightedOperatorId()).subscribe(v => (published = v));
      expect(published).toBe(true);
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
