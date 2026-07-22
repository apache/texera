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
import { By } from "@angular/platform-browser";
import { RowModalComponent } from "./result-panel-modal.component";
import { PanelResizeService } from "../../service/workflow-result/panel-resize/panel-resize.service";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { of } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { NotificationService } from "../../../common/service/notification/notification.service";

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

  it("should fall back to the text view (not the raw remote URL) when media-proxy request fails", () => {
    const remoteUrl = "https://example.com/clip.mp4";
    const entries = (component as any).buildRowEntries({ vid: remoteUrl });
    const entry = entries[0];
    expect(entry.isVideo).toBe(true);

    const req = httpMock.expectOne(
      `${AppSettings.getApiEndpoint()}/huggingface/media-proxy?url=${encodeURIComponent(remoteUrl)}`
    );
    req.error(new ProgressEvent("error"));

    expect(entry.mediaSrc).toBe("");
    expect(entry.isVideo).toBe(false);
    expect(entry.isImage).toBe(false);
    expect(entry.isAudio).toBe(false);
  });

  it("should not fetch media-proxy for non-media remote URLs", () => {
    const remoteUrl = "https://example.com/some-text-value";
    (component as any).buildRowEntries({ text: remoteUrl });
    httpMock.expectNone(`${AppSettings.getApiEndpoint()}/huggingface/media-proxy?url=${encodeURIComponent(remoteUrl)}`);
  });

  it("should revoke blob URLs on destroy", () => {
    const revokeSpy = vi.spyOn(URL, "revokeObjectURL");
    (component as any).allocatedBlobUrls.push("blob:url-1", "blob:url-2");
    component.ngOnDestroy();
    expect(revokeSpy).toHaveBeenCalledWith("blob:url-1");
    expect(revokeSpy).toHaveBeenCalledWith("blob:url-2");
    revokeSpy.mockRestore();
  });

  it("prettyRowJson should return pretty-printed JSON of currentDisplayRowData", () => {
    component.currentDisplayRowData = { name: "test", value: 42 };
    expect(component.prettyRowJson).toBe(JSON.stringify({ name: "test", value: 42 }, null, 2));
  });

  it("trackByEntryKey should return the entry key", () => {
    expect(component.trackByEntryKey(0, { key: "myKey" })).toBe("myKey");
    expect(component.trackByEntryKey(5, { key: "another" })).toBe("another");
  });

  it("should JSON-stringify non-string values in buildRowEntries", () => {
    const entries = (component as any).buildRowEntries({ count: 42, arr: [1, 2, 3] });
    expect(entries[0].value).toBe("42");
    expect(entries[0].isImage).toBe(false);
    expect(entries[0].isVideo).toBe(false);
    expect(entries[0].isAudio).toBe(false);
    expect(entries[1].value).toBe("[1,2,3]");
    expect(entries[1].mediaSrc).toBe("[1,2,3]");
  });

  it("should not update row data when tuple is null", () => {
    vi.spyOn(workflowResultServiceSpy, "getPaginatedResultService").mockReturnValueOnce({
      selectTuple: vi.fn().mockReturnValue(of({ tuple: null })),
    } as any);
    component.currentDisplayRowData = { existing: "data" };
    component.ngOnChanges();
    expect(component.currentDisplayRowData).toEqual({ existing: "data" });
  });

  it("should call notificationService.success on successful clipboard copy", async () => {
    const notifService = TestBed.inject(NotificationService);
    const successSpy = vi.spyOn(notifService, "success").mockImplementation(() => undefined as any);
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText: vi.fn().mockResolvedValue(undefined) },
      configurable: true,
      writable: true,
    });
    component.copyText("hello world");
    await new Promise(r => setTimeout(r, 0));
    expect(successSpy).toHaveBeenCalledWith("Copied to clipboard");
    successSpy.mockRestore();
  });

  it("should call notificationService.error on clipboard copy failure", async () => {
    const notifService = TestBed.inject(NotificationService);
    const errorSpy = vi.spyOn(notifService, "error").mockImplementation(() => undefined as any);
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText: vi.fn().mockRejectedValue(new Error("denied")) },
      configurable: true,
      writable: true,
    });
    component.copyText("hello world");
    await new Promise(r => setTimeout(r, 0));
    expect(errorSpy).toHaveBeenCalledWith("Failed to copy");
    errorSpy.mockRestore();
  });
});

describe("RowModalComponent (with pre-loaded rowData)", () => {
  let component: RowModalComponent;
  let httpMock: HttpTestingController;

  const rowData = { id: "123", imgUrl: "data:image/png;base64,abc" };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RowModalComponent, HttpClientTestingModule],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: { operatorId: "op-2", rowIndex: 0, rowData } },
        { provide: NzModalRef, useValue: { getConfig: () => ({}), close: vi.fn() } },
        {
          provide: WorkflowResultService,
          useValue: { getPaginatedResultService: vi.fn().mockReturnValue(null) },
        },
        { provide: PanelResizeService, useValue: { pageSize: 10 } },
      ],
    }).compileComponents();

    httpMock = TestBed.inject(HttpTestingController);
    const fixture = TestBed.createComponent(RowModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should initialize currentDisplayRowData from rowData in modal data", () => {
    expect(component.currentDisplayRowData).toEqual(rowData);
  });

  it("should build rowEntries immediately for data URL images in rowData", () => {
    const imageEntry = component.rowEntries.find(e => e.key === "imgUrl");
    expect(imageEntry?.isImage).toBe(true);
    expect(imageEntry?.mediaSrc).toBe(rowData.imgUrl);
  });

  it("should build rowEntries for all fields in rowData", () => {
    expect(component.rowEntries.length).toBe(2);
    const idEntry = component.rowEntries.find(e => e.key === "id");
    expect(idEntry?.value).toBe("123");
  });
});

describe("RowModalComponent (template rendering)", () => {
  let component: RowModalComponent;
  let fixture: ComponentFixture<RowModalComponent>;
  let httpMock: HttpTestingController;

  const rowData = {
    video: "data:video/mp4;base64,vid123",
    image: "data:image/png;base64,img123",
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RowModalComponent, HttpClientTestingModule],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: { operatorId: "op-3", rowIndex: 0, rowData } },
        { provide: NzModalRef, useValue: { getConfig: () => ({}), close: vi.fn() } },
        {
          provide: WorkflowResultService,
          useValue: { getPaginatedResultService: vi.fn().mockReturnValue(null) },
        },
        { provide: PanelResizeService, useValue: { pageSize: 10 } },
      ],
    }).compileComponents();

    httpMock = TestBed.inject(HttpTestingController);
    fixture = TestBed.createComponent(RowModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should bind the video element's src to the video entry's mediaSrc", () => {
    const videoEl = fixture.debugElement.query(By.css("video")).nativeElement as HTMLVideoElement;
    expect(videoEl.src).toBe(rowData.video);
  });

  it("should bind the image element's src to the image entry's mediaSrc", () => {
    const imgEl = fixture.debugElement.query(By.css("img")).nativeElement as HTMLImageElement;
    expect(imgEl.src).toBe(rowData.image);
  });

  it("should call copyText with the entry's value when its Copy button is clicked", () => {
    const copyTextSpy = vi.spyOn(component, "copyText").mockImplementation(() => undefined);
    const copyButtons = fixture.debugElement.queryAll(By.css(".row-detail-header button"));
    const videoEntryButton = copyButtons[0];

    videoEntryButton.triggerEventHandler("click", null);

    expect(copyTextSpy).toHaveBeenCalledWith(rowData.video);
    copyTextSpy.mockRestore();
  });
});
