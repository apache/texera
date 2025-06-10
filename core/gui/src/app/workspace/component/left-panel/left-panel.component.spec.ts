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
import { LeftPanelComponent } from "./left-panel.component";
import { mockPoint, mockScanPredicate } from "../../service/workflow-graph/model/mock-workflow-data";
import { VersionsListComponent } from "./versions-list/versions-list.component";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { RouterTestingModule } from "@angular/router/testing";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("LeftPanelComponent", () => {
  let component: LeftPanelComponent;

  let workflowActionService: WorkflowActionService;
  let fixture: ComponentFixture<LeftPanelComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, RouterTestingModule.withRoutes([])],
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        ...commonTestProviders,
      ],
      declarations: [LeftPanelComponent],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LeftPanelComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("should switch to versions frame component when get all versions is clicked", () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add one operator
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    // highlight the first operator
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);
    fixture.detectChanges();

    //the operator shall be highlighted
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().length).toBe(1);

    // click on versions display
    component.openFrame(2);
    fixture.detectChanges();

    // all the elements shall be un-highlighted
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().length).toBe(0);
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs().length).toBe(0);

    // the component should switch to versions display
    expect(component.currentComponent).toBe(VersionsListComponent);
  });
});
