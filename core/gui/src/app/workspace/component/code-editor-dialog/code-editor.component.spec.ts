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
import { CodeEditorComponent } from "./code-editor.component";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { mockJavaUDFPredicate, mockPoint } from "../../service/workflow-graph/model/mock-workflow-data";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";

describe("CodeEditorDialogComponent", () => {
  let component: CodeEditorComponent;
  let fixture: ComponentFixture<CodeEditorComponent>;
  let workflowActionService: WorkflowActionService;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [CodeEditorComponent],
      providers: [
        WorkflowActionService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
      imports: [HttpClientTestingModule],
    }).compileComponents();
  }));

  beforeEach(() => {
    workflowActionService = TestBed.inject(WorkflowActionService);
    workflowActionService.addOperator(mockJavaUDFPredicate, mockPoint);
    workflowActionService.getJointGraphWrapper().highlightOperators(mockJavaUDFPredicate.operatorID);
    fixture = TestBed.createComponent(CodeEditorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  // it("should create a websocket when the editor is opened", () => {
  //   let socketInstance = component.getLanguageServerSocket();
  //   expect(socketInstance).toBeTruthy();
  // });
});
