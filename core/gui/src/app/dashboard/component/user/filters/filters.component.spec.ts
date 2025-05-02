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

import { FiltersComponent } from "./filters.component";
import { StubOperatorMetadataService } from "src/app/workspace/service/operator-metadata/stub-operator-metadata.service";
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import { StubWorkflowPersistService } from "src/app/common/service/workflow-persist/stub-workflow-persist.service";
import { testWorkflowEntries } from "../../user-dashboard-test-fixtures";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { JWT_OPTIONS, JwtHelperService } from "@auth0/angular-jwt";
import { FormsModule } from "@angular/forms";
import { HttpClientTestingModule } from "@angular/common/http/testing";

describe("FiltersComponent", () => {
  let component: FiltersComponent;
  let fixture: ComponentFixture<FiltersComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [FiltersComponent],
      providers: [
        JwtHelperService,
        { provide: JWT_OPTIONS, useValue: {} },
        { provide: WorkflowPersistService, useValue: new StubWorkflowPersistService(testWorkflowEntries) },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
      ],
      imports: [NzDropDownModule, FormsModule, HttpClientTestingModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(FiltersComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("parses manually entered mtime", () => {
    component.masterFilterList = ["mtime: 2022-01-22 ~ 2022-04-21"];
    expect(component.selectedMtime).toEqual([new Date(2022, 0, 22), new Date(2022, 3, 21)]);
  });

  it("parses manually entered ctime", () => {
    component.masterFilterList = ["ctime: 2022-01-22 ~ 2022-04-21"];
    expect(component.selectedCtime).toEqual([new Date(2022, 0, 22), new Date(2022, 3, 21)]);
  });

  it("preserves ordering when parsing drop down", () => {
    component.masterFilterList = ["keyword", "ctime: 2022-01-22 ~ 2022-04-21", "keyword 2"];
    component.selectedCtime = [new Date(2022, 2, 22), new Date(2022, 4, 21)];
    component.buildMasterFilterList();
    expect(component.masterFilterList).toEqual(["keyword", "ctime: 2022-03-22 ~ 2022-05-21", "keyword 2"]);
    component.masterFilterList = [...component.masterFilterList, "another keyword"];
    expect(component.masterFilterList).toEqual([
      "keyword",
      "ctime: 2022-03-22 ~ 2022-05-21",
      "keyword 2",
      "another keyword",
    ]);
  });
});
