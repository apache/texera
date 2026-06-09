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
import { RowModalComponent } from "./result-panel-modal.component";
import { PanelResizeService } from "../../service/workflow-result/panel-resize/panel-resize.service";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { of } from "rxjs";

describe("RowModalComponent", () => {
  let component: RowModalComponent;
  let fixture: ComponentFixture<RowModalComponent>;

  const mockTupleResult = { tuple: { id: "123", value: "test_data" } };
  const workflowResultServiceSpy = {
    getPaginatedResultService: vi.fn().mockReturnValue({
      selectTuple: vi.fn().mockReturnValue(of(mockTupleResult)),
    }),
  };

  const resizeServiceSpy = {
    pageSize: 10,
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RowModalComponent],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: { operatorId: "op-1", rowIndex: 3 } },
        { provide: NzModalRef, useValue: { getConfig: () => ({}), close: vi.fn() } },
        { provide: WorkflowResultService, useValue: workflowResultServiceSpy },
        { provide: PanelResizeService, useValue: resizeServiceSpy },
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RowModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("should populate row data on ngOnChanges", () => {
    component.ngOnChanges();
    expect(component.currentDisplayRowData).toEqual(mockTupleResult.tuple);
  });
});
