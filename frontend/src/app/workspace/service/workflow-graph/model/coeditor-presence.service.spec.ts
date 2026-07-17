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

import { fakeAsync, TestBed, tick } from "@angular/core/testing";

import { CoeditorPresenceService } from "./coeditor-presence.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzDropdownMenuComponent, NzDropDownModule } from "ng-zorro-antd/dropdown";
import { CoeditorUserIconComponent } from "../../../component/menu/coeditor-user-icon/coeditor-user-icon.component";
import { WorkflowActionService } from "./workflow-action.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { UserService } from "../../../../common/service/user/user.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { OperatorMetadataService } from "../../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../operator-metadata/stub-operator-metadata.service";
import { Coeditor, CoeditorState, Role } from "../../../../common/type/user";
import { WorkflowGraph } from "./workflow-graph";
import { JointGraphWrapper } from "./joint-graph-wrapper";
import { mockPoint, mockScanPredicate } from "./mock-workflow-data";
import { Awareness } from "y-protocols/awareness";
import * as joint from "jointjs";

describe("CoeditorPresenceService", () => {
  let service: CoeditorPresenceService;
  let workflowActionService: WorkflowActionService;
  let texeraGraph: WorkflowGraph;
  let jointGraph: joint.dia.Graph;
  let jointGraphWrapper: JointGraphWrapper;
  let awareness: Awareness;

  const operatorId = mockScanPredicate.operatorID;
  const fakeIntervalId = 123 as unknown as ReturnType<typeof setInterval>;
  let addHighlightSpy: ReturnType<typeof vi.spyOn>;
  let deleteHighlightSpy: ReturnType<typeof vi.spyOn>;
  let setCurrentEditingSpy: ReturnType<typeof vi.spyOn>;
  let removeCurrentEditingSpy: ReturnType<typeof vi.spyOn>;
  let setPropertyChangedSpy: ReturnType<typeof vi.spyOn>;
  let removePropertyChangedSpy: ReturnType<typeof vi.spyOn>;
  let highlightOperatorsSpy: ReturnType<typeof vi.spyOn>;
  let unhighlightOperatorsSpy: ReturnType<typeof vi.spyOn>;

  const makeCoeditor = (clientNum: number, color = "#e91e63"): Coeditor => ({
    uid: clientNum,
    name: `coeditor-${clientNum}`,
    email: `coeditor-${clientNum}@test.com`,
    role: Role.REGULAR,
    color,
    comment: "",
    joiningReason: "",
    clientId: clientNum.toString(),
  });

  const makeState = (user: Coeditor, overrides: Partial<CoeditorState> = {}): CoeditorState => ({
    user,
    isActive: true,
    userCursor: { x: 10, y: 20 },
    ...overrides,
  });

  const pointerId = (coeditor: Coeditor): string => "pointer_" + coeditor.clientId;

  const emitAwarenessChange = (added: number[], updated: number[], removed: number[]) => {
    awareness.emit("change", [{ added, updated, removed }, "test"]);
  };

  const addRemoteCoeditor = (clientNum: number, state: CoeditorState) => {
    awareness.getStates().set(clientNum, state);
    emitAwarenessChange([clientNum], [], []);
  };

  const updateRemoteCoeditor = (clientNum: number, state: CoeditorState) => {
    awareness.getStates().set(clientNum, state);
    emitAwarenessChange([], [clientNum], []);
  };

  const removeRemoteCoeditor = (clientNum: number) => {
    awareness.getStates().delete(clientNum);
    emitAwarenessChange([], [], [clientNum]);
  };

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, NzDropDownModule, CoeditorUserIconComponent],
      providers: [
        WorkflowActionService,
        CoeditorPresenceService,
        NzDropdownMenuComponent,
        { provide: UserService, useClass: StubUserService },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        ...commonTestProviders,
      ],
    });
    service = TestBed.inject(CoeditorPresenceService);
    workflowActionService = TestBed.inject(WorkflowActionService);

    texeraGraph = workflowActionService.getTexeraGraph() as WorkflowGraph;
    jointGraph = workflowActionService.getJointGraph();
    jointGraphWrapper = workflowActionService.getJointGraphWrapper();
    awareness = texeraGraph.sharedModel.awareness;

    // No joint paper is attached in unit tests; fake the minimal surface the cursor-update path touches.
    const fakePaper = {
      findViewByModel: vi.fn().mockReturnValue({ setInteractivity: vi.fn() }),
    } as unknown as joint.dia.Paper;
    vi.spyOn(jointGraphWrapper, "getMainJointPaper").mockReturnValue(fakePaper);
    addHighlightSpy = vi.spyOn(jointGraphWrapper, "addCoeditorOperatorHighlight").mockImplementation(() => {});
    deleteHighlightSpy = vi.spyOn(jointGraphWrapper, "deleteCoeditorOperatorHighlight").mockImplementation(() => {});
    setCurrentEditingSpy = vi.spyOn(jointGraphWrapper, "setCurrentEditing").mockReturnValue(fakeIntervalId);
    removeCurrentEditingSpy = vi.spyOn(jointGraphWrapper, "removeCurrentEditing").mockImplementation(() => {});
    setPropertyChangedSpy = vi.spyOn(jointGraphWrapper, "setPropertyChanged").mockImplementation(() => {});
    removePropertyChangedSpy = vi.spyOn(jointGraphWrapper, "removePropertyChanged").mockImplementation(() => {});
    highlightOperatorsSpy = vi.spyOn(workflowActionService, "highlightOperators").mockImplementation(() => {});
    unhighlightOperatorsSpy = vi.spyOn(workflowActionService, "unhighlightOperators").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  describe("co-editor join and leave", () => {
    it("registers a remote co-editor and draws its cursor pointer", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA));

      expect(service.coeditors).toEqual([coeditorA]);
      const pointer = jointGraph.getCell(pointerId(coeditorA)) as joint.dia.Element;
      expect(pointer).toBeTruthy();
      expect(pointer.position().x).toBe(10);
      expect(pointer.position().y).toBe(20);
    });

    it("does not register the same co-editor twice", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA));
      addRemoteCoeditor(1001, makeState(coeditorA));
      expect(service.coeditors).toEqual([coeditorA]);
    });

    it("ignores added events for the local client", () => {
      const localCoeditor = makeCoeditor(awareness.clientID);
      awareness.getStates().set(awareness.clientID, makeState(localCoeditor));
      emitAwarenessChange([awareness.clientID], [], []);
      expect(service.coeditors).toEqual([]);
    });

    it("unregisters a co-editor and removes its cursor when its state is dropped", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA));
      expect(jointGraph.getCell(pointerId(coeditorA))).toBeTruthy();

      removeRemoteCoeditor(1001);

      expect(service.coeditors).toEqual([]);
      expect(jointGraph.getCell(pointerId(coeditorA))).toBeFalsy();
    });

    it("keeps a co-editor whose state is still present when a removal event arrives", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA));

      // removal event without the state actually being gone from the awareness map
      emitAwarenessChange([], [], [1001]);

      expect(service.coeditors).toEqual([coeditorA]);
    });

    it("registers an unseen co-editor arriving through an update event", () => {
      const coeditorA = makeCoeditor(1001);
      updateRemoteCoeditor(1001, makeState(coeditorA));
      expect(service.coeditors).toEqual([coeditorA]);
    });

    it("ignores update events from the local client", () => {
      awareness.setLocalState(makeState(makeCoeditor(awareness.clientID)));
      expect(service.coeditors).toEqual([]);
    });
  });

  describe("re-observing after a new yDoc is loaded", () => {
    it("tears down and re-registers remote co-editors", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA, { highlighted: ["op-x"] }));
      expect(addHighlightSpy).toHaveBeenCalledTimes(1);

      texeraGraph.newYDocLoadedSubject.next(undefined);

      // the old registration is removed (highlight cleaned up), then rebuilt from awareness states
      expect(deleteHighlightSpy).toHaveBeenCalledWith(coeditorA, "op-x");
      expect(addHighlightSpy).toHaveBeenCalledTimes(2);
      expect(service.coeditors).toEqual([coeditorA]);
    });

    it("does not register the local user during first-time registration", () => {
      awareness.setLocalState(makeState(makeCoeditor(awareness.clientID)));
      texeraGraph.newYDocLoadedSubject.next(undefined);
      expect(service.coeditors).toEqual([]);
    });
  });

  describe("shadowing mode", () => {
    it("highlights the shadowed co-editor's operator and re-emits its open code editor", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA, { currentlyEditing: operatorId, editingCode: true }));
      const opened: { operatorId: string }[] = [];
      service.getCoeditorOpenedCodeEditorSubject().subscribe(e => opened.push(e));

      service.shadowCoeditor(coeditorA);

      expect(service.shadowingModeEnabled).toBe(true);
      expect(service.shadowingCoeditor).toBe(coeditorA);
      expect(highlightOperatorsSpy).toHaveBeenCalledWith(false, operatorId);
      expect(opened).toEqual([{ operatorId }]);

      service.stopShadowing();
      expect(service.shadowingModeEnabled).toBe(false);
    });

    it("does not re-emit code-editor state when the shadowed co-editor has none", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const coeditorA = makeCoeditor(1001);
      const coeditorB = makeCoeditor(1002);
      addRemoteCoeditor(1001, makeState(coeditorA, { currentlyEditing: operatorId }));
      addRemoteCoeditor(1002, makeState(coeditorB));
      const opened: { operatorId: string }[] = [];
      service.getCoeditorOpenedCodeEditorSubject().subscribe(e => opened.push(e));

      service.shadowCoeditor(coeditorA); // editing an operator, but not editing code
      service.shadowCoeditor(coeditorB); // not editing anything

      expect(highlightOperatorsSpy).toHaveBeenCalledTimes(1);
      expect(highlightOperatorsSpy).toHaveBeenCalledWith(false, operatorId);
      expect(opened).toEqual([]);
    });

    it("stops shadowing when the shadowed co-editor leaves", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA));
      service.shadowCoeditor(coeditorA);
      expect(service.shadowingModeEnabled).toBe(true);

      removeRemoteCoeditor(1001);

      expect(service.shadowingModeEnabled).toBe(false);
      expect(service.coeditors).toEqual([]);
    });
  });

  describe("currently-editing propagation", () => {
    beforeEach(() => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
    });

    it("shows and clears the currently-editing indicator", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA, { currentlyEditing: operatorId }));
      expect(setCurrentEditingSpy).toHaveBeenCalledWith(coeditorA, operatorId);

      updateRemoteCoeditor(1001, makeState(coeditorA));
      expect(removeCurrentEditingSpy).toHaveBeenCalledWith(coeditorA, operatorId, fakeIntervalId);
    });

    it("highlights and unhighlights the edited operator while shadowing", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA));
      service.shadowCoeditor(coeditorA);

      updateRemoteCoeditor(1001, makeState(coeditorA, { currentlyEditing: operatorId }));
      expect(highlightOperatorsSpy).toHaveBeenCalledWith(false, operatorId);

      updateRemoteCoeditor(1001, makeState(coeditorA));
      expect(unhighlightOperatorsSpy).toHaveBeenCalledWith(operatorId);
    });

    it("ignores currently-editing targets that are not in the graph", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA, { currentlyEditing: "not-a-real-operator" }));
      expect(setCurrentEditingSpy).not.toHaveBeenCalled();

      updateRemoteCoeditor(1001, makeState(coeditorA, { currentlyEditing: undefined }));
      expect(removeCurrentEditingSpy).not.toHaveBeenCalled();
    });
  });

  describe("operator highlight propagation", () => {
    it("mirrors incremental highlight and unhighlight changes", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA, { highlighted: ["op-a"] }));
      expect(addHighlightSpy).toHaveBeenCalledTimes(1);
      expect(addHighlightSpy).toHaveBeenCalledWith(coeditorA, "op-a");

      updateRemoteCoeditor(1001, makeState(coeditorA, { highlighted: ["op-a", "op-b"] }));
      expect(addHighlightSpy).toHaveBeenCalledTimes(2);
      expect(addHighlightSpy).toHaveBeenLastCalledWith(coeditorA, "op-b");
      expect(deleteHighlightSpy).not.toHaveBeenCalled();

      // identical highlight set: no UI churn
      updateRemoteCoeditor(1001, makeState(coeditorA, { highlighted: ["op-a", "op-b"] }));
      expect(addHighlightSpy).toHaveBeenCalledTimes(2);
      expect(deleteHighlightSpy).not.toHaveBeenCalled();

      updateRemoteCoeditor(1001, makeState(coeditorA, { highlighted: ["op-b"] }));
      expect(deleteHighlightSpy).toHaveBeenCalledTimes(1);
      expect(deleteHighlightSpy).toHaveBeenCalledWith(coeditorA, "op-a");

      updateRemoteCoeditor(1001, makeState(coeditorA));
      expect(deleteHighlightSpy).toHaveBeenCalledTimes(2);
      expect(deleteHighlightSpy).toHaveBeenLastCalledWith(coeditorA, "op-b");
    });
  });

  describe("property-changed propagation", () => {
    it("flashes the property-changed indicator and clears it after the timeout", fakeAsync(() => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA, { changed: "op-a" }));
      expect(setPropertyChangedSpy).toHaveBeenCalledTimes(1);
      expect(setPropertyChangedSpy).toHaveBeenCalledWith(coeditorA, "op-a");

      // same changed value: no second flash
      updateRemoteCoeditor(1001, makeState(coeditorA, { changed: "op-a" }));
      expect(setPropertyChangedSpy).toHaveBeenCalledTimes(1);

      expect(removePropertyChangedSpy).not.toHaveBeenCalled();
      tick(2000);
      expect(removePropertyChangedSpy).toHaveBeenCalledWith(coeditorA, "op-a");
    }));
  });

  describe("code editor open/close propagation", () => {
    beforeEach(() => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
    });

    it("emits open and close events for the shadowed co-editor", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA, { currentlyEditing: operatorId }));
      service.shadowCoeditor(coeditorA);
      const opened: { operatorId: string }[] = [];
      const closed: { operatorId: string }[] = [];
      service.getCoeditorOpenedCodeEditorSubject().subscribe(e => opened.push(e));
      service.getCoeditorClosedCodeEditorSubject().subscribe(e => closed.push(e));

      updateRemoteCoeditor(1001, makeState(coeditorA, { currentlyEditing: operatorId, editingCode: true }));
      expect(opened).toEqual([{ operatorId }]);
      expect(closed).toEqual([]);

      updateRemoteCoeditor(1001, makeState(coeditorA, { currentlyEditing: operatorId, editingCode: false }));
      expect(closed).toEqual([{ operatorId }]);
    });

    it("tracks code-editor state silently when not shadowing", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA, { currentlyEditing: operatorId }));
      const opened: { operatorId: string }[] = [];
      const closed: { operatorId: string }[] = [];
      service.getCoeditorOpenedCodeEditorSubject().subscribe(e => opened.push(e));
      service.getCoeditorClosedCodeEditorSubject().subscribe(e => closed.push(e));

      updateRemoteCoeditor(1001, makeState(coeditorA, { currentlyEditing: operatorId, editingCode: true }));
      updateRemoteCoeditor(1001, makeState(coeditorA, { currentlyEditing: operatorId, editingCode: false }));

      expect(opened).toEqual([]);
      expect(closed).toEqual([]);
    });
  });

  describe("cursor propagation", () => {
    it("moves the pointer when an active co-editor's cursor position changes", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA));

      updateRemoteCoeditor(1001, makeState(coeditorA, { userCursor: { x: 50, y: 60 } }));

      const pointer = jointGraph.getCell(pointerId(coeditorA)) as joint.dia.Element;
      expect(pointer.position().x).toBe(50);
      expect(pointer.position().y).toBe(60);
    });

    it("removes the pointer when the co-editor becomes inactive", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA));
      expect(jointGraph.getCell(pointerId(coeditorA))).toBeTruthy();

      updateRemoteCoeditor(1001, makeState(coeditorA, { isActive: false }));

      expect(jointGraph.getCell(pointerId(coeditorA))).toBeFalsy();
    });

    it("does not draw a pointer for an inactive or colorless co-editor", () => {
      const inactive = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(inactive, { isActive: false }));
      expect(jointGraph.getCell(pointerId(inactive))).toBeFalsy();

      const colorless = { ...makeCoeditor(1002), color: undefined };
      addRemoteCoeditor(1002, makeState(colorless));
      expect(jointGraph.getCell(pointerId(colorless))).toBeFalsy();
    });

    it("removes the pointer without redrawing when the co-editor loses its color", () => {
      const coeditorA = makeCoeditor(1001);
      addRemoteCoeditor(1001, makeState(coeditorA));
      expect(jointGraph.getCell(pointerId(coeditorA))).toBeTruthy();

      updateRemoteCoeditor(1001, makeState({ ...coeditorA, color: undefined }));

      expect(jointGraph.getCell(pointerId(coeditorA))).toBeFalsy();
    });
  });
});
