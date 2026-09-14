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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { OverboxService } from "../../service/overbox/overbox.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OverboxDialogComponent } from "./overbox-dialog.component";
import { NzModalService } from "ng-zorro-antd/modal";

describe("OverboxDialogComponent", () => {
  let fixture: ComponentFixture<OverboxDialogComponent>;
  const modalData: { id?: string } = {};
  const modalService = { create: vi.fn() };

  beforeEach(async () => {
    delete modalData.id;
    vi.clearAllMocks();
    await TestBed.configureTestingModule({
      imports: [OverboxDialogComponent, HttpClientTestingModule],
      providers: [
        ...commonTestProviders,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: NZ_MODAL_DATA, useValue: modalData },
        { provide: NzModalRef, useValue: { close: vi.fn() } },
        { provide: NzModalService, useValue: modalService },
        { provide: OverboxService, useValue: { create: vi.fn(), update: vi.fn() } },
      ],
    }).compileComponents();
    fixture = TestBed.createComponent(OverboxDialogComponent);
    fixture.detectChanges();
  });

  afterEach(() => fixture?.destroy());

  it("offers the original fixed palette including yellow without a custom color input", () => {
    const element = fixture.nativeElement as HTMLElement;
    expect(element.querySelector('input[type="color"]')).toBeNull();
    expect(element.querySelectorAll(".color-swatch")).toHaveLength(8);
    const yellow = element.querySelector('[aria-label="Choose color #fadb14"]') as HTMLButtonElement;
    yellow.click();
    fixture.detectChanges();
    expect(fixture.componentInstance.color).toBe("#fadb14");
    expect(element.textContent).toContain("Save section");
  });

  it("keeps saving disabled until a section name is provided", () => {
    const nameInput = fixture.nativeElement.querySelector("#overbox-name") as HTMLInputElement;
    expect(nameInput.placeholder).toBe("");
    expect(fixture.nativeElement.textContent).not.toContain("After saving");
    const save = fixture.nativeElement.querySelector('button[type="submit"]') as HTMLButtonElement;
    expect(save.disabled).toBe(true);
    fixture.componentInstance.name = "Preparation";
    fixture.detectChanges();
    expect(save.disabled).toBe(false);
  });

  it("offers saving an existing section box as a reusable section", () => {
    const actions = TestBed.inject(WorkflowActionService);
    actions.addCommentBox({
      commentBoxID: "commentBox-existing",
      comments: [],
      commentBoxPosition: { x: 20, y: 20 },
      overbox: { name: "Preparation", color: "#1677ff", width: 300, height: 180 },
    });
    modalData.id = "commentBox-existing";
    fixture.destroy();
    fixture = TestBed.createComponent(OverboxDialogComponent);
    fixture.detectChanges();
    const reusableButton = Array.from<HTMLButtonElement>(fixture.nativeElement.querySelectorAll("button")).find(
      button => button.textContent.includes("Save as reusable section")
    )!;
    reusableButton.click();

    expect(modalService.create).toHaveBeenCalledWith(expect.objectContaining({ nzTitle: "Preparation" }));
  });
});
