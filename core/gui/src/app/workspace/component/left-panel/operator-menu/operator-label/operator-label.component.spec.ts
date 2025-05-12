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
import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { OperatorLabelComponent } from "./operator-label.component";
import { OperatorMetadataService } from "../../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../../service/operator-metadata/stub-operator-metadata.service";
import { mockScanSourceSchema } from "../../../../service/operator-metadata/mock-operator-metadata.data";
import { By } from "@angular/platform-browser";
import { WorkflowActionService } from "../../../../service/workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../../../../service/undo-redo/undo-redo.service";
import { RouterTestingModule } from "@angular/router/testing";

describe("OperatorLabelComponent", () => {
  const mockOperatorData = mockScanSourceSchema;
  let component: OperatorLabelComponent;
  let fixture: ComponentFixture<OperatorLabelComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [OperatorLabelComponent],
      imports: [RouterTestingModule.withRoutes([])],
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
      ],
    }).compileComponents();
  }));

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
});
