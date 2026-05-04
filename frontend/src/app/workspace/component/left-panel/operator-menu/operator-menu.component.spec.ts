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

import { mockScanSourceSchema } from "../../../service/operator-metadata/mock-operator-metadata.data";
import { UndoRedoService } from "../../../service/undo-redo/undo-redo.service";
import { DragDropService } from "../../../service/drag-drop/drag-drop.service";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { OperatorMenuComponent } from "./operator-menu.component";
import { OperatorLabelComponent } from "./operator-label/operator-label.component";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { RouterTestingModule } from "@angular/router/testing";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { JointUIService } from "../../../service/joint-ui/joint-ui.service";
import { WorkflowUtilService } from "../../../service/workflow-graph/util/workflow-util.service";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzCollapseModule } from "ng-zorro-antd/collapse";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { NO_ERRORS_SCHEMA } from "@angular/core";

describe("OperatorPanelComponent", () => {
  let component: OperatorMenuComponent;
  let fixture: ComponentFixture<OperatorMenuComponent>;

  beforeEach(async () => {
    TestBed.overrideComponent(OperatorMenuComponent, {
      set: {
        template: "",
      },
    });

    await TestBed.configureTestingModule({
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        DragDropService,
        WorkflowActionService,
        UndoRedoService,
        WorkflowUtilService,
        JointUIService,
        ...commonTestProviders,
      ],
      imports: [
        OperatorMenuComponent,
        OperatorLabelComponent,
        NzDropDownModule,
        NzCollapseModule,
        BrowserAnimationsModule,
        RouterTestingModule.withRoutes([]),
      ],
      schemas: [NO_ERRORS_SCHEMA],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OperatorMenuComponent);
    fixture.detectChanges();
    component = fixture.componentInstance;
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("should search an operator by its user friendly name", () => {
    component.searchInputValue = "Source: Scan";
    component.onInput({ target: { value: "Source: Scan" } } as unknown as Event);

    expect(component.autocompleteOptions.length).toBe(1);
    expect(component.autocompleteOptions[0]).toBe(mockScanSourceSchema);
  });

  it("should support fuzzy search on operator user friendly name", () => {
    component.searchInputValue = "scan";
    component.onInput({ target: { value: "scan" } } as unknown as Event);

    expect(component.autocompleteOptions.length).toBe(1);
    expect(component.autocompleteOptions[0]).toBe(mockScanSourceSchema);
  });

  it("should clear the search box when an operator from search box is dropped", () => {
    component.searchInputValue = "scan";
    component.onInput({ target: { value: "scan" } } as unknown as Event);

    const dragDropService = TestBed.inject(DragDropService);
    (dragDropService as any).operatorDroppedSubject.next();

    expect(component.searchInputValue).toBeFalsy();
  });
});
