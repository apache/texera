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

import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { FormControl } from "@angular/forms";
import { FieldTypeConfig } from "@ngx-formly/core";
import { AppSettings } from "../../../common/app-setting";
import { HuggingFaceAudioUploadComponent } from "./hugging-face-audio-upload.component";

const API = "api";

describe("HuggingFaceAudioUploadComponent", () => {
  let component: HuggingFaceAudioUploadComponent;
  let httpTestingController: HttpTestingController;
  let formControl: FormControl;

  function makeFileEvent(file: File | null): Event {
    const input = document.createElement("input");
    if (file) {
      Object.defineProperty(input, "files", { value: [file] });
    }
    return { target: input } as unknown as Event;
  }

  function makeFileEventWithInput(file: File | null): { event: Event; input: HTMLInputElement } {
    const input = document.createElement("input");
    if (file) {
      Object.defineProperty(input, "files", { value: [file] });
    }
    return { event: { target: input } as unknown as Event, input };
  }

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HuggingFaceAudioUploadComponent, HttpClientTestingModule],
    }).compileComponents();

    vi.spyOn(AppSettings, "getApiEndpoint").mockReturnValue(API);

    const fixture = TestBed.createComponent(HuggingFaceAudioUploadComponent);
    component = fixture.componentInstance;
    formControl = new FormControl("");
    component.field = { formControl, key: "audioInput", model: {} } as unknown as FieldTypeConfig;
    httpTestingController = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTestingController.verify();
  });

  it("should be defined", () => {
    expect(component).toBeDefined();
  });

  // ── ngOnInit ──

  describe("ngOnInit", () => {
    it("should set fileName from existing formControl value", () => {
      formControl.setValue("/uploads/my-clip.wav");
      component.ngOnInit();
      expect(component.fileName).toBe("my-clip.wav");
    });

    it("should set fileName to 'Selected audio' for data:audio values", () => {
      formControl.setValue("data:audio/wav;base64,abc123");
      component.ngOnInit();
      expect(component.fileName).toBe("Selected audio");
    });

    it("should not set fileName when formControl is empty", () => {
      formControl.setValue("");
      component.ngOnInit();
      expect(component.fileName).toBe("");
    });

    it("should not set fileName when formControl is whitespace", () => {
      formControl.setValue("   ");
      component.ngOnInit();
      expect(component.fileName).toBe("");
    });
  });

  // ── previewSrc ──

  describe("previewSrc", () => {
    it("should return empty string when formControl is empty and no local preview", () => {
      expect(component.previewSrc).toBe("");
    });

    it("should return server preview URL for a stored path", () => {
      formControl.setValue("/uploads/clip.wav");
      expect(component.previewSrc).toBe(`${API}/huggingface/audio-preview?path=%2Fuploads%2Fclip.wav`);
    });

    it("should return data:audio value as-is", () => {
      const dataUrl = "data:audio/wav;base64,abc123";
      formControl.setValue(dataUrl);
      expect(component.previewSrc).toBe(dataUrl);
    });

    it("should return empty string for whitespace-only value", () => {
      formControl.setValue("   ");
      expect(component.previewSrc).toBe("");
    });
  });

  // ── File upload ──

  describe("onFileSelected", () => {
    it("should reject a non-audio file", async () => {
      const file = new File(["data"], "doc.pdf", { type: "application/pdf" });
      await component.onFileSelected(makeFileEvent(file));

      expect(component.errorMessage).toBe("Choose an audio file.");
      expect(formControl.value).toBe("");
    });

    it("should upload an audio file and set formControl value", async () => {
      const file = new File(["audio-data"], "clip.wav", { type: "audio/wav" });
      const uploadPromise = component.onFileSelected(makeFileEvent(file));

      const req = httpTestingController.expectOne(
        r => r.method === "POST" && r.url.includes("/huggingface/upload-audio")
      );
      req.flush({ path: "/tmp/clip.wav", fileName: "clip.wav" });
      await uploadPromise;

      expect(formControl.value).toBe("/tmp/clip.wav");
      expect(component.fileName).toBe("clip.wav");
      expect(component.isUploading).toBe(false);
    });

    it("should guard against concurrent uploads", async () => {
      component.isUploading = true;
      const file = new File(["audio-data"], "clip.wav", { type: "audio/wav" });
      await component.onFileSelected(makeFileEvent(file));

      httpTestingController.expectNone(r => r.url.includes("/huggingface/upload-audio"));
      expect(formControl.value).toBe("");
    });

    it("should do nothing when no file is selected", async () => {
      await component.onFileSelected(makeFileEvent(null));

      httpTestingController.expectNone(r => r.url.includes("/huggingface/upload-audio"));
      expect(formControl.value).toBe("");
      expect(component.errorMessage).toBe("");
    });

    it("should set isUploading while upload is in progress", async () => {
      const file = new File(["audio-data"], "clip.wav", { type: "audio/wav" });
      const uploadPromise = component.onFileSelected(makeFileEvent(file));

      expect(component.isUploading).toBe(true);

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/upload-audio"));
      req.flush({ path: "/tmp/clip.wav", fileName: "clip.wav" });
      await uploadPromise;

      expect(component.isUploading).toBe(false);
    });

    it("should clear error message before new upload", async () => {
      component.errorMessage = "previous error";
      const file = new File(["audio-data"], "clip.wav", { type: "audio/wav" });
      const uploadPromise = component.onFileSelected(makeFileEvent(file));

      expect(component.errorMessage).toBe("");

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/upload-audio"));
      req.flush({ path: "/tmp/clip.wav", fileName: "clip.wav" });
      await uploadPromise;
    });

    it("should show error on upload failure", async () => {
      const file = new File(["audio-data"], "clip.wav", { type: "audio/wav" });
      const uploadPromise = component.onFileSelected(makeFileEvent(file));

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/upload-audio"));
      req.error(new ProgressEvent("error"));
      await uploadPromise;

      expect(component.errorMessage).toBe("Could not upload this audio file.");
      expect(component.isUploading).toBe(false);
      expect(formControl.value).toBe("");
    });

    it("should use file.name as fallback when response.fileName is empty", async () => {
      const file = new File(["audio-data"], "my-clip.mp3", { type: "audio/mp3" });
      const uploadPromise = component.onFileSelected(makeFileEvent(file));

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/upload-audio"));
      req.flush({ path: "/tmp/my-clip.mp3", fileName: "" });
      await uploadPromise;

      expect(component.fileName).toBe("my-clip.mp3");
    });

    it("should update the model when key is a string", async () => {
      const model: Record<string, unknown> = {};
      component.field = { formControl, key: "audioInput", model } as unknown as FieldTypeConfig;

      const file = new File(["audio-data"], "clip.wav", { type: "audio/wav" });
      const uploadPromise = component.onFileSelected(makeFileEvent(file));

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/upload-audio"));
      req.flush({ path: "/tmp/clip.wav", fileName: "clip.wav" });
      await uploadPromise;

      expect(model["audioInput"]).toBe("/tmp/clip.wav");
    });

    it("should send correct Content-Type and URL", async () => {
      const file = new File(["audio-data"], "my clip.wav", { type: "audio/wav" });
      const uploadPromise = component.onFileSelected(makeFileEvent(file));

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/upload-audio"));
      expect(req.request.url).toContain("filename=my%20clip.wav");
      expect(req.request.headers.get("Content-Type")).toBe("application/octet-stream");
      req.flush({ path: "/tmp/clip.wav", fileName: "clip.wav" });
      await uploadPromise;
    });
  });

  // ── clearAudio ──

  describe("clearAudio", () => {
    it("should reset all state", () => {
      component.fileName = "clip.wav";
      component.errorMessage = "some error";
      formControl.setValue("/tmp/clip.wav");

      const input = document.createElement("input");
      component.clearAudio(input);

      expect(component.fileName).toBe("");
      expect(component.errorMessage).toBe("");
      expect(component.isUploading).toBe(false);
      expect(formControl.value).toBe("");
    });

    it("should preserve error message when clearError is false", () => {
      component.errorMessage = "upload failed";
      const input = document.createElement("input");
      component.clearAudio(input, false);

      expect(component.errorMessage).toBe("upload failed");
    });

    it("should clear model value when key is a string", () => {
      const model: Record<string, unknown> = { audioInput: "/tmp/clip.wav" };
      component.field = { formControl, key: "audioInput", model } as unknown as FieldTypeConfig;

      const input = document.createElement("input");
      component.clearAudio(input);

      expect(model["audioInput"]).toBe("");
    });

    it("should mark formControl as dirty and touched", () => {
      const input = document.createElement("input");
      component.clearAudio(input);

      expect(formControl.dirty).toBe(true);
      expect(formControl.touched).toBe(true);
    });
  });

  // ── ngOnDestroy ──

  describe("ngOnDestroy", () => {
    it("should not throw on destroy", () => {
      expect(() => component.ngOnDestroy()).not.toThrow();
    });
  });
});
