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

import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { MiniMapComponent } from "./mini-map.component";
import { WorkflowEditorComponent } from "../workflow-editor.component";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { JointUIService } from "../../../service/joint-ui/joint-ui.service";
import { UndoRedoService } from "../../../service/undo-redo/undo-redo.service";
import { WorkflowUtilService } from "../../../service/workflow-graph/util/workflow-util.service";
import { DragDropModule } from "@angular/cdk/drag-drop";
import { commonTestProviders } from "../../../../common/testing/test-utils";

describe("MiniMapComponent", () => {
  let fixture: ComponentFixture<MiniMapComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [MiniMapComponent, WorkflowEditorComponent],
      providers: [
        WorkflowActionService,
        WorkflowUtilService,
        JointUIService,
        UndoRedoService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        ...commonTestProviders,
      ],
      imports: [HttpClientTestingModule, DragDropModule],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MiniMapComponent);
    TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(fixture.componentInstance).toBeTruthy();
  });
});
