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
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { of } from "rxjs";
import { AppSettings } from "../../../common/app-setting";

describe("RowModalComponent", () => {
  let component: RowModalComponent;
  let fixture: ComponentFixture<RowModalComponent>;
  let httpMock: HttpTestingController;

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
      imports: [RowModalComponent, HttpClientTestingModule],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: { operatorId: "op-1", rowIndex: 3 } },
        { provide: NzModalRef, useValue: { getConfig: () => ({}), close: vi.fn() } },
        { provide: WorkflowResultService, useValue: workflowResultServiceSpy },
        { provide: PanelResizeService, useValue: resizeServiceSpy },
      ],
    }).compileComponents();

    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
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

  it("should use data URL directly without fetching for base64 media", () => {
    const dataUrl = "data:image/png;base64,abc123";
    (component as any).buildRowEntries({ img: dataUrl });
    httpMock.expectNone(`${AppSettings.getApiEndpoint()}/huggingface/media-proxy`);
    const entry = (component as any).buildRowEntries({ img: dataUrl })[0];
    expect(entry.mediaSrc).toBe(dataUrl);
    expect(entry.isImage).toBe(true);
  });

  it("should fetch remote image URL via media-proxy and set blob URL on success", () => {
    const createObjectURLSpy = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:fake-url");
    const remoteUrl = "https://example.com/photo.png";
    const entries = (component as any).buildRowEntries({ img: remoteUrl });
    const entry = entries[0];

    expect(entry.mediaSrc).toBe("");
    expect(entry.isImage).toBe(true);

    const req = httpMock.expectOne(
      `${AppSettings.getApiEndpoint()}/huggingface/media-proxy?url=${encodeURIComponent(remoteUrl)}`
    );
    req.flush(new Blob(["fake"], { type: "image/png" }));

    expect(createObjectURLSpy).toHaveBeenCalled();
    expect(entry.mediaSrc).toBe("blob:fake-url");
    createObjectURLSpy.mockRestore();
  });

  it("should fall back to raw URL when media-proxy request fails", () => {
    const remoteUrl = "https://example.com/clip.mp4";
    const entries = (component as any).buildRowEntries({ vid: remoteUrl });
    const entry = entries[0];

    const req = httpMock.expectOne(
      `${AppSettings.getApiEndpoint()}/huggingface/media-proxy?url=${encodeURIComponent(remoteUrl)}`
    );
    req.error(new ProgressEvent("error"));

    expect(entry.mediaSrc).toBe(remoteUrl);
  });

  it("should not fetch media-proxy for non-media remote URLs", () => {
    const remoteUrl = "https://example.com/some-text-value";
    (component as any).buildRowEntries({ text: remoteUrl });
    httpMock.expectNone(
      `${AppSettings.getApiEndpoint()}/huggingface/media-proxy?url=${encodeURIComponent(remoteUrl)}`
    );
  });

  it("should revoke blob URLs on destroy", () => {
    const revokeSpy = vi.spyOn(URL, "revokeObjectURL");
    (component as any).allocatedBlobUrls.push("blob:url-1", "blob:url-2");
    component.ngOnDestroy();
    expect(revokeSpy).toHaveBeenCalledWith("blob:url-1");
    expect(revokeSpy).toHaveBeenCalledWith("blob:url-2");
    revokeSpy.mockRestore();
  });
});
