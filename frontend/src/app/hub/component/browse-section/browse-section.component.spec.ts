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
import { BrowseSectionComponent } from "./browse-section.component";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { ChangeDetectorRef } from "@angular/core";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { DashboardEntry } from "../../../dashboard/type/dashboard-entry";
import {
  HUB_DATASET_RESULT_DETAIL,
  HUB_WORKFLOW_RESULT_DETAIL,
  USER_DATASET,
  USER_WORKSPACE,
} from "../../../app-routing.constant";

describe("BrowseSectionComponent", () => {
  let component: BrowseSectionComponent;
  let fixture: ComponentFixture<BrowseSectionComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [BrowseSectionComponent],
      providers: [
        { provide: WorkflowPersistService, useValue: {} },
        { provide: DatasetService, useValue: {} },
        { provide: ChangeDetectorRef, useValue: {} },
        ...commonTestProviders,
      ],
    });
    fixture = TestBed.createComponent(BrowseSectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("entityRoutes initialization", () => {
    it("routes owned workflows to the user workspace", () => {
      component.currentUid = 1;
      component.entities = [{ id: 100, type: "workflow", accessibleUserIds: [1] } as unknown as DashboardEntry];
      component.ngOnInit();
      expect(component.entityRoutes[100]).toEqual([USER_WORKSPACE, "100"]);
    });

    it("routes non-owned workflows to the hub workflow detail page", () => {
      component.currentUid = 1;
      component.entities = [{ id: 101, type: "workflow", accessibleUserIds: [2] } as unknown as DashboardEntry];
      component.ngOnInit();
      expect(component.entityRoutes[101]).toEqual([HUB_WORKFLOW_RESULT_DETAIL, "101"]);
    });

    it("routes owned datasets to the user dataset page", () => {
      component.currentUid = 1;
      component.entities = [{ id: 200, type: "dataset", accessibleUserIds: [1] } as unknown as DashboardEntry];
      component.ngOnInit();
      expect(component.entityRoutes[200]).toEqual([USER_DATASET, "200"]);
    });

    it("routes non-owned datasets to the hub dataset detail page", () => {
      component.currentUid = 1;
      component.entities = [{ id: 201, type: "dataset", accessibleUserIds: [2] } as unknown as DashboardEntry];
      component.ngOnInit();
      expect(component.entityRoutes[201]).toEqual([HUB_DATASET_RESULT_DETAIL, "201"]);
    });
  });
});
