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

import { WorkflowUtilService } from "../../../../service/workflow-graph/util/workflow-util.service";
import { JointUIService } from "../../../../service/joint-ui/joint-ui.service";
import { DragDropService } from "../../../../service/drag-drop/drag-drop.service";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { OperatorLabelComponent } from "./operator-label.component";
import { OperatorMetadataService } from "../../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../../service/operator-metadata/stub-operator-metadata.service";
import { mockScanSourceSchema } from "../../../../service/operator-metadata/mock-operator-metadata.data";
import { By } from "@angular/platform-browser";
import { WorkflowActionService } from "../../../../service/workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../../../../service/undo-redo/undo-redo.service";
import { RouterTestingModule } from "@angular/router/testing";
import { commonTestProviders } from "../../../../../common/testing/test-utils";
import { CdkDrag, CdkDragDrop, CdkDragStart } from "@angular/cdk/drag-drop";

describe("OperatorLabelComponent", () => {
  const mockOperatorData = mockScanSourceSchema;
  let component: OperatorLabelComponent;
  let fixture: ComponentFixture<OperatorLabelComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [OperatorLabelComponent, RouterTestingModule.withRoutes([])],
      providers: [
        DragDropService,
        JointUIService,
        WorkflowUtilService,
        WorkflowActionService,
        UndoRedoService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OperatorLabelComponent);
    component = fixture.componentInstance;

    // use one mock operator schema as input to construct the operator label
    component.operator = mockOperatorData;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("should display operator user friendly name on the UI", () => {
    const element = <HTMLElement>fixture.debugElement.query(By.css(".text")).nativeElement;
    expect(element.textContent?.trim()).toEqual(mockOperatorData.additionalMetadata.userFriendlyName);
  });

  /**
   * Dragging a label onto the canvas is the whole point of this component, and both of its handlers
   * ran uncounted: the suite above only renders the label. These drive the two CdkDrag outputs the
   * template binds rather than calling the methods directly, so the bindings are exercised too.
   */
  describe("drag handling", () => {
    let dragDropService: DragDropService;
    let cdkDrag: CdkDrag;

    beforeEach(() => {
      dragDropService = TestBed.inject(DragDropService);
      cdkDrag = fixture.debugElement.query(By.directive(CdkDrag)).injector.get(CdkDrag);
    });

    it("announces the dragged operator's type, not its display name", () => {
      // The canvas needs the type to instantiate an operator; "Source: Scan" would not resolve.
      const dragStartedSpy = vi.spyOn(dragDropService, "dragStarted").mockImplementation(() => {});

      cdkDrag.started.emit({ source: cdkDrag } as unknown as CdkDragStart);

      expect(dragStartedSpy).toHaveBeenCalledExactlyOnceWith(mockOperatorData.operatorType);
    });

    it("refuses to start a drag while the workflow is read-only", () => {
      const dragStartedSpy = vi.spyOn(dragDropService, "dragStarted").mockImplementation(() => {});
      TestBed.inject(WorkflowActionService).disableWorkflowModification();
      fixture.detectChanges();

      // the label is visibly marked undraggable ...
      const label = <HTMLElement>fixture.debugElement.query(By.css(".operator-label")).nativeElement;
      expect(label.classList.contains("disable-drag-drop")).toBe(true);

      // ... and a drag that fires anyway is dropped on the floor
      cdkDrag.started.emit({ source: cdkDrag } as unknown as CdkDragStart);
      expect(dragStartedSpy).not.toHaveBeenCalled();
    });

    it("forwards the drop point of a completed drag", () => {
      const dragDroppedSpy = vi.spyOn(dragDropService, "dragDropped").mockImplementation(() => {});

      // x and y differ so a handler that swapped them would not go unnoticed
      cdkDrag.dropped.emit({ dropPoint: { x: 137, y: 421 } } as unknown as CdkDragDrop<unknown>);

      expect(dragDroppedSpy).toHaveBeenCalledExactlyOnceWith({ x: 137, y: 421 });
    });
  });
});
