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
      // ngOnInit fires an authenticated blob fetch for the server path
      httpTestingController.expectOne(r => r.url.includes("/huggingface/audio-preview"));
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

    it("should return empty for a stored server path (blob URL loaded asynchronously)", () => {
      formControl.setValue("/uploads/clip.wav");
      expect(component.previewSrc).toBe("");
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

  // ── loadServerAudioPreview (via ngOnInit) ──

  describe("loadServerAudioPreview", () => {
    it("should set localPreviewUrl on successful blob fetch", async () => {
      const blobUrl = "blob:http://localhost/fake-audio";
      vi.spyOn(URL, "createObjectURL").mockReturnValue(blobUrl);

      formControl.setValue("/uploads/clip.wav");
      component.ngOnInit();

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/audio-preview"));
      expect(req.request.responseType).toBe("blob");
      req.flush(new Blob(["audio-data"], { type: "audio/wav" }));

      // Allow microtask (promise .then) to settle
      await new Promise(resolve => setTimeout(resolve, 0));

      expect(component.previewSrc).toBe(blobUrl);
    });

    it("should set errorMessage on blob fetch failure", async () => {
      formControl.setValue("/uploads/clip.wav");
      component.ngOnInit();

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/audio-preview"));
      req.error(new ProgressEvent("error"));

      await new Promise(resolve => setTimeout(resolve, 0));

      expect(component.errorMessage).toBe("Could not load audio preview.");
    });

    it("should discard blob fetch result if formControl value changed", async () => {
      const blobUrl = "blob:http://localhost/fake-audio";
      vi.spyOn(URL, "createObjectURL").mockReturnValue(blobUrl);

      formControl.setValue("/uploads/clip.wav");
      component.ngOnInit();

      // User cleared the field before fetch completes
      formControl.setValue("");

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/audio-preview"));
      req.flush(new Blob(["audio-data"], { type: "audio/wav" }));

      await new Promise(resolve => setTimeout(resolve, 0));

      expect(component.previewSrc).toBe("");
    });

    it("should discard error if formControl value changed before fetch fails", async () => {
      formControl.setValue("/uploads/clip.wav");
      component.ngOnInit();

      formControl.setValue("");

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/audio-preview"));
      req.error(new ProgressEvent("error"));

      await new Promise(resolve => setTimeout(resolve, 0));

      expect(component.errorMessage).toBe("");
    });

    it("should not fetch for data:audio values in ngOnInit", () => {
      formControl.setValue("data:audio/wav;base64,abc123");
      component.ngOnInit();

      httpTestingController.expectNone(r => r.url.includes("/huggingface/audio-preview"));
    });

    it("should encode server path in the fetch URL", () => {
      formControl.setValue("/uploads/my clip.wav");
      component.ngOnInit();

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/audio-preview"));
      expect(req.request.url).toContain("path=%2Fuploads%2Fmy%20clip.wav");
    });
  });

  // ── previewSrc with localPreviewUrl ──

  describe("previewSrc with localPreviewUrl", () => {
    it("should return localPreviewUrl when set via file upload", async () => {
      const blobUrl = "blob:http://localhost/local-preview";
      vi.spyOn(URL, "createObjectURL").mockReturnValue(blobUrl);
      vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});

      const file = new File(["audio-data"], "clip.wav", { type: "audio/wav" });
      const uploadPromise = component.onFileSelected(makeFileEvent(file));

      // After file selection, localPreviewUrl should be set
      expect(component.previewSrc).toBe(blobUrl);

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/upload-audio"));
      req.flush({ path: "/tmp/clip.wav", fileName: "clip.wav" });
      await uploadPromise;
    });
  });

  // ── Stale upload guards ──

  describe("stale upload guards", () => {
    it("should discard successful upload if cleared during flight", async () => {
      const blobUrl = "blob:http://localhost/local-preview";
      vi.spyOn(URL, "createObjectURL").mockReturnValue(blobUrl);
      vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});

      const { event, input } = makeFileEventWithInput(new File(["audio-data"], "clip.wav", { type: "audio/wav" }));
      const uploadPromise = component.onFileSelected(event);

      // Clear while upload is in flight
      component.clearAudio(input);

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/upload-audio"));
      req.flush({ path: "/tmp/clip.wav", fileName: "clip.wav" });
      await uploadPromise;

      // Upload result should be discarded — formControl stays empty
      expect(formControl.value).toBe("");
    });

    it("should discard upload error if cleared during flight", async () => {
      const blobUrl = "blob:http://localhost/local-preview";
      vi.spyOn(URL, "createObjectURL").mockReturnValue(blobUrl);
      vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});

      const { event, input } = makeFileEventWithInput(new File(["audio-data"], "clip.wav", { type: "audio/wav" }));
      const uploadPromise = component.onFileSelected(event);

      component.clearAudio(input);

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/upload-audio"));
      req.error(new ProgressEvent("error"));
      await uploadPromise;

      // Error should be discarded — errorMessage stays empty (clearAudio clears it)
      expect(component.errorMessage).toBe("");
    });
  });

  // ── ngOnDestroy ──

  describe("ngOnDestroy", () => {
    it("should not throw on destroy", () => {
      expect(() => component.ngOnDestroy()).not.toThrow();
    });

    it("should revoke localPreviewUrl on destroy", async () => {
      const blobUrl = "blob:http://localhost/local-preview";
      vi.spyOn(URL, "createObjectURL").mockReturnValue(blobUrl);
      const revokeSpy = vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});

      const file = new File(["audio-data"], "clip.wav", { type: "audio/wav" });
      const uploadPromise = component.onFileSelected(makeFileEvent(file));

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/upload-audio"));
      req.flush({ path: "/tmp/clip.wav", fileName: "clip.wav" });
      await uploadPromise;

      component.ngOnDestroy();
      expect(revokeSpy).toHaveBeenCalledWith(blobUrl);
    });
  });

  // ── getDisplayName edge cases ──

  describe("getDisplayName (via ngOnInit)", () => {
    it("should extract filename from path with forward slashes", () => {
      formControl.setValue("/path/to/my-clip.wav");
      component.ngOnInit();
      expect(component.fileName).toBe("my-clip.wav");
      httpTestingController.expectOne(r => r.url.includes("/huggingface/audio-preview"));
    });

    it("should extract filename from path with backslashes", () => {
      formControl.setValue("C:\\uploads\\my-clip.wav");
      component.ngOnInit();
      expect(component.fileName).toBe("my-clip.wav");
      httpTestingController.expectOne(r => r.url.includes("/huggingface/audio-preview"));
    });

    it("should return 'Selected audio' for path ending with separator", () => {
      formControl.setValue("/uploads/");
      component.ngOnInit();
      expect(component.fileName).toBe("Selected audio");
      httpTestingController.expectOne(r => r.url.includes("/huggingface/audio-preview"));
    });

    it("should return filename for flat name without path", () => {
      formControl.setValue("clip.wav");
      component.ngOnInit();
      expect(component.fileName).toBe("clip.wav");
      httpTestingController.expectOne(r => r.url.includes("/huggingface/audio-preview"));
    });
  });

  // ── Upload marks formControl as dirty and touched ──

  describe("formControl state after upload", () => {
    it("should mark formControl as dirty and touched after successful upload", async () => {
      const file = new File(["audio-data"], "clip.wav", { type: "audio/wav" });
      const uploadPromise = component.onFileSelected(makeFileEvent(file));

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/upload-audio"));
      req.flush({ path: "/tmp/clip.wav", fileName: "clip.wav" });
      await uploadPromise;

      expect(formControl.dirty).toBe(true);
      expect(formControl.touched).toBe(true);
    });

    it("should mark formControl as dirty and touched after clear", () => {
      formControl.setValue("/tmp/clip.wav");
      const input = document.createElement("input");
      component.clearAudio(input);

      expect(formControl.dirty).toBe(true);
      expect(formControl.touched).toBe(true);
    });
  });

  // ── Upload updates model ──

  describe("model update on upload", () => {
    it("should not update model when key is not a string", async () => {
      const model: Record<string, unknown> = {};
      component.field = { formControl, key: 42 as any, model } as unknown as FieldTypeConfig;

      const file = new File(["audio-data"], "clip.wav", { type: "audio/wav" });
      const uploadPromise = component.onFileSelected(makeFileEvent(file));

      const req = httpTestingController.expectOne(r => r.url.includes("/huggingface/upload-audio"));
      req.flush({ path: "/tmp/clip.wav", fileName: "clip.wav" });
      await uploadPromise;

      expect(formControl.value).toBe("/tmp/clip.wav");
      expect(model[42 as any]).toBeUndefined();
    });
  });

  // ── revokePreviewUrl no-op ──

  describe("revokePreviewUrl", () => {
    it("should not throw when destroying without any preview", () => {
      expect(() => component.ngOnDestroy()).not.toThrow();
    });
  });

  // ── previewSrc with non-audio data URL ──

  describe("previewSrc edge cases", () => {
    it("should return empty for non-audio data URL", () => {
      formControl.setValue("data:image/png;base64,abc123");
      expect(component.previewSrc).toBe("");
    });

    it("should return data:audio value with different audio type", () => {
      const mp3DataUrl = "data:audio/mp3;base64,abc123";
      formControl.setValue(mp3DataUrl);
      expect(component.previewSrc).toBe(mp3DataUrl);
    });
  });

  // ── Multiple consecutive uploads ──

  describe("consecutive uploads", () => {
    it("should replace previous upload value with new upload", async () => {
      // First upload
      const file1 = new File(["audio-1"], "first.wav", { type: "audio/wav" });
      const upload1 = component.onFileSelected(makeFileEvent(file1));
      const req1 = httpTestingController.expectOne(r => r.url.includes("/huggingface/upload-audio"));
      req1.flush({ path: "/tmp/first.wav", fileName: "first.wav" });
      await upload1;

      expect(formControl.value).toBe("/tmp/first.wav");
      expect(component.fileName).toBe("first.wav");

      // Second upload
      const file2 = new File(["audio-2"], "second.wav", { type: "audio/wav" });
      const upload2 = component.onFileSelected(makeFileEvent(file2));
      const req2 = httpTestingController.expectOne(r => r.url.includes("/huggingface/upload-audio"));
      req2.flush({ path: "/tmp/second.wav", fileName: "second.wav" });
      await upload2;

      expect(formControl.value).toBe("/tmp/second.wav");
      expect(component.fileName).toBe("second.wav");
    });
  });
});
