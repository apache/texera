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
import { FormControl } from "@angular/forms";
import { HuggingFaceImageUploadComponent } from "./hugging-face-image-upload.component";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("HuggingFaceImageUploadComponent", () => {
  let component: HuggingFaceImageUploadComponent;
  let fixture: ComponentFixture<HuggingFaceImageUploadComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HuggingFaceImageUploadComponent],
      providers: [...commonTestProviders],
    }).compileComponents();

    fixture = TestBed.createComponent(HuggingFaceImageUploadComponent);
    component = fixture.componentInstance;
    component.field = {
      props: {},
      formControl: new FormControl(""),
      key: "image",
      model: {},
    } as any;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("derived view state", () => {
    it("reports no image when formControl is empty", () => {
      expect(component.hasImage).toBe(false);
      expect(component.previewSrc).toBe("");
      expect(component.displayFileName).toBe("");
    });

    it("reports an image when formControl holds a data URL", () => {
      component.formControl.setValue("data:image/jpeg;base64,AAA");
      expect(component.hasImage).toBe(true);
      expect(component.previewSrc).toBe("data:image/jpeg;base64,AAA");
      expect(component.displayFileName).toBe("Uploaded image");
    });

    it("prefers the explicit filename over the fallback label", () => {
      component.formControl.setValue("data:image/jpeg;base64,AAA");
      component.fileName = "cat.jpg";
      expect(component.displayFileName).toBe("cat.jpg");
    });
  });

  describe("onFileSelected", () => {
    function makeFileInput(file?: File): HTMLInputElement {
      const input = document.createElement("input");
      input.type = "file";
      if (file) {
        Object.defineProperty(input, "files", {
          value: [file] as unknown as FileList,
          configurable: true,
        });
      }
      return input;
    }

    it("clears prior error and returns early when no file is provided", async () => {
      component.errorMessage = "previous error";
      const input = makeFileInput();
      await component.onFileSelected({ target: input } as unknown as Event);
      expect(component.errorMessage).toBe("");
      expect(component.formControl.value).toBe("");
    });

    it("rejects non-image files and resets the input", async () => {
      const txtFile = new File(["hi"], "note.txt", { type: "text/plain" });
      const input = makeFileInput(txtFile);
      await component.onFileSelected({ target: input } as unknown as Event);
      expect(component.errorMessage).toBe("Choose an image file.");
      expect(component.hasImage).toBe(false);
    });

    it("reports an error when image compression fails", async () => {
      // jsdom's Image never fires onload/onerror, so compressImage would hang
      // forever. Stub FileReader so it synchronously fires onerror, which
      // makes compressImage reject and exercises the catch branch.
      const realFileReader = globalThis.FileReader;
      class FailingFileReader {
        onload: ((e: Event) => void) | null = null;
        onerror: ((e: Event) => void) | null = null;
        readAsDataURL() {
          queueMicrotask(() => this.onerror?.(new Event("error")));
        }
      }
      (globalThis as any).FileReader = FailingFileReader;
      try {
        const imgFile = new File(["fake"], "broken.png", { type: "image/png" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.errorMessage).toBe("Could not prepare this image. Try a smaller image file.");
        expect(component.hasImage).toBe(false);
      } finally {
        (globalThis as any).FileReader = realFileReader;
      }
    });
  });

  describe("clearImage", () => {
    it("resets file state, the form control, and any model value", () => {
      (component.field as any).model = { image: "data:image/jpeg;base64,AAA" };
      component.formControl.setValue("data:image/jpeg;base64,AAA");
      component.fileName = "cat.jpg";
      component.errorMessage = "some error";

      const input = document.createElement("input");
      input.type = "file";

      component.clearImage(input);

      expect(component.fileName).toBe("");
      expect(component.errorMessage).toBe("");
      expect(input.value).toBe("");
      expect(component.formControl.value).toBe("");
      expect(component.formControl.dirty).toBe(true);
      expect(component.formControl.touched).toBe(true);
      expect((component.model as any).image).toBe("");
    });
  });

  describe("hasImage edge cases", () => {
    it("returns false for a server path string", () => {
      component.formControl.setValue("/uploads/photo.png");
      expect(component.hasImage).toBe(false);
    });

    it("returns false for null value", () => {
      component.formControl.setValue(null);
      expect(component.hasImage).toBe(false);
    });

    it("returns false for non-image data URL", () => {
      component.formControl.setValue("data:audio/wav;base64,AAA");
      expect(component.hasImage).toBe(false);
    });

    it("returns true for data:image/png URL", () => {
      component.formControl.setValue("data:image/png;base64,AAA");
      expect(component.hasImage).toBe(true);
    });
  });

  describe("previewSrc", () => {
    it("returns data URL when formControl has image", () => {
      component.formControl.setValue("data:image/jpeg;base64,AAA");
      expect(component.previewSrc).toBe("data:image/jpeg;base64,AAA");
    });

    it("returns empty string when formControl has no image", () => {
      component.formControl.setValue("");
      expect(component.previewSrc).toBe("");
    });

    it("returns empty string for server path", () => {
      component.formControl.setValue("/uploads/photo.png");
      expect(component.previewSrc).toBe("");
    });
  });

  describe("displayFileName", () => {
    it("returns empty string when no image and no fileName", () => {
      expect(component.displayFileName).toBe("");
    });

    it("returns 'Uploaded image' when image present but no fileName", () => {
      component.formControl.setValue("data:image/jpeg;base64,AAA");
      expect(component.displayFileName).toBe("Uploaded image");
    });

    it("returns fileName when set, even without image", () => {
      component.fileName = "photo.jpg";
      expect(component.displayFileName).toBe("photo.jpg");
    });
  });

  // ── Shared mocking helpers ──────────────────────────────────────────────
  // These helpers let us drive the private compressImage / renderCompressedDataUrl
  // pipeline end-to-end through onFileSelected.

  function makeFileInput(file?: File): HTMLInputElement {
    const input = document.createElement("input");
    input.type = "file";
    if (file) {
      Object.defineProperty(input, "files", {
        value: [file] as unknown as FileList,
        configurable: true,
      });
    }
    return input;
  }

  interface CompressionMockOptions {
    /** Value returned by FileReader.result. Default: a valid data URL. */
    readerResult?: string | ArrayBuffer | null;
    /** If true, FileReader fires onerror instead of onload. */
    readerError?: boolean;
    /** If true, Image fires onerror instead of onload. */
    imageError?: boolean;
    /** Image natural dimensions. Default: 100 x 100. */
    imageWidth?: number;
    imageHeight?: number;
    /** Value(s) returned by canvas.toDataURL on each call. Cycles if shorter than call count. */
    canvasDataUrls?: string[];
    /** If true, canvas.getContext returns null. */
    nullCanvasContext?: boolean;
  }

  /**
   * Installs fake FileReader, Image, and canvas stubs so the compression
   * pipeline runs synchronously via microtasks. Returns a teardown function.
   */
  function installCompressionMocks(opts: CompressionMockOptions = {}): () => void {
    const savedFileReader = globalThis.FileReader;
    const savedImage = globalThis.Image;

    const readerResult = "readerResult" in opts ? opts.readerResult : "data:image/jpeg;base64,AAAA";

    class FakeFileReader {
      onload: ((e: Event) => void) | null = null;
      onerror: ((e: Event) => void) | null = null;
      result: string | ArrayBuffer | null = readerResult!;
      readAsDataURL() {
        queueMicrotask(() => {
          if (opts.readerError) {
            this.onerror?.(new Event("error"));
          } else {
            this.onload?.(new Event("load"));
          }
        });
      }
    }
    (globalThis as any).FileReader = FakeFileReader;

    class FakeImage {
      onload: (() => void) | null = null;
      onerror: (() => void) | null = null;
      width = opts.imageWidth ?? 100;
      height = opts.imageHeight ?? 100;
      set src(_: string) {
        queueMicrotask(() => {
          if (opts.imageError) {
            this.onerror?.();
          } else {
            this.onload?.();
          }
        });
      }
    }
    (globalThis as any).Image = FakeImage;

    let toDataUrlCallIndex = 0;
    const canvasDataUrls = opts.canvasDataUrls ?? ["data:image/jpeg;base64,SMALL"];

    const origCreateElement = document.createElement.bind(document);
    vi.spyOn(document, "createElement").mockImplementation((tag: string) => {
      if (tag === "canvas") {
        return {
          width: 0,
          height: 0,
          getContext: () =>
            opts.nullCanvasContext
              ? null
              : {
                  drawImage: () => {},
                },
          toDataURL: () => {
            const url = canvasDataUrls[toDataUrlCallIndex % canvasDataUrls.length];
            toDataUrlCallIndex++;
            return url;
          },
        } as any;
      }
      return origCreateElement(tag);
    });

    return () => {
      (globalThis as any).FileReader = savedFileReader;
      (globalThis as any).Image = savedImage;
      vi.restoreAllMocks();
    };
  }

  // ── Successful upload ─────────────────────────────────────────────────

  describe("successful upload", () => {
    it("sets fileName, formControl, model, and marks dirty/touched", async () => {
      const teardown = installCompressionMocks();
      try {
        const imgFile = new File(["fake"], "sunset.jpg", { type: "image/jpeg" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);

        expect(component.fileName).toBe("sunset.jpg");
        expect(component.formControl.value).toBe("data:image/jpeg;base64,SMALL");
        expect(component.hasImage).toBe(true);
        expect(component.formControl.dirty).toBe(true);
        expect(component.formControl.touched).toBe(true);
        expect((component.model as any).image).toBe("data:image/jpeg;base64,SMALL");
        expect(component.errorMessage).toBe("");
      } finally {
        teardown();
      }
    });

    it("clears a previous error on successful upload", async () => {
      component.errorMessage = "previous failure";
      const teardown = installCompressionMocks();
      try {
        const imgFile = new File(["fake"], "ok.png", { type: "image/png" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.errorMessage).toBe("");
      } finally {
        teardown();
      }
    });
  });

  // ── compressImage rejection paths ─────────────────────────────────────

  describe("compressImage rejection paths", () => {
    it("rejects when FileReader.result is not a string (ArrayBuffer)", async () => {
      const teardown = installCompressionMocks({ readerResult: new ArrayBuffer(8) });
      try {
        const imgFile = new File(["fake"], "pic.png", { type: "image/png" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.errorMessage).toBe("Could not prepare this image. Try a smaller image file.");
        expect(component.hasImage).toBe(false);
      } finally {
        teardown();
      }
    });

    it("rejects when FileReader.result is null", async () => {
      const teardown = installCompressionMocks({ readerResult: null });
      try {
        const imgFile = new File(["fake"], "pic.png", { type: "image/png" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.errorMessage).toBe("Could not prepare this image. Try a smaller image file.");
      } finally {
        teardown();
      }
    });

    it("rejects when Image fires onerror", async () => {
      const teardown = installCompressionMocks({ imageError: true });
      try {
        const imgFile = new File(["fake"], "corrupt.jpg", { type: "image/jpeg" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.errorMessage).toBe("Could not prepare this image. Try a smaller image file.");
        expect(component.hasImage).toBe(false);
      } finally {
        teardown();
      }
    });
  });

  // ── renderCompressedDataUrl edge cases ────────────────────────────────

  describe("renderCompressedDataUrl (via onFileSelected)", () => {
    it("returns empty when canvas context is null", async () => {
      const teardown = installCompressionMocks({ nullCanvasContext: true });
      try {
        const imgFile = new File(["fake"], "pic.png", { type: "image/png" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);
        // bestDataUrl starts as "" → compressed is "" → fails startsWith check → rejects
        expect(component.errorMessage).toBe("Could not prepare this image. Try a smaller image file.");
      } finally {
        teardown();
      }
    });

    it("accepts a data URL that fits within the size limit on first attempt", async () => {
      const teardown = installCompressionMocks({
        canvasDataUrls: ["data:image/jpeg;base64,FIT"],
      });
      try {
        const imgFile = new File(["fake"], "small.jpg", { type: "image/jpeg" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.formControl.value).toBe("data:image/jpeg;base64,FIT");
        expect(component.errorMessage).toBe("");
      } finally {
        teardown();
      }
    });

    it("reduces quality when the first toDataURL result exceeds the size limit", async () => {
      // First call returns oversized URL, second call returns a small one
      const oversized = "data:image/jpeg;base64," + "A".repeat(50000);
      const small = "data:image/jpeg;base64,OK";
      const teardown = installCompressionMocks({
        canvasDataUrls: [oversized, small],
      });
      try {
        const imgFile = new File(["fake"], "large.jpg", { type: "image/jpeg" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.formControl.value).toBe(small);
        expect(component.errorMessage).toBe("");
      } finally {
        teardown();
      }
    });

    it("reduces dimension when quality loop alone is not enough", async () => {
      // Quality has 5 steps (0.75, 0.65, 0.55, 0.45, 0.35).
      // After exhausting quality at the first maxDimension, it shrinks dimension and retries.
      // We make the first 5 calls oversized (quality loop at dim=512), then return small on the 6th (dim=384).
      const oversized = "data:image/jpeg;base64," + "A".repeat(50000);
      const small = "data:image/jpeg;base64,SHRUNK";
      const teardown = installCompressionMocks({
        canvasDataUrls: [oversized, oversized, oversized, oversized, oversized, small],
      });
      try {
        const imgFile = new File(["fake"], "huge.jpg", { type: "image/jpeg" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.formControl.value).toBe(small);
      } finally {
        teardown();
      }
    });

    it("rejects when compressed result never fits within size limit", async () => {
      // All toDataURL calls return oversized results
      const oversized = "data:image/jpeg;base64," + "A".repeat(50000);
      const teardown = installCompressionMocks({
        canvasDataUrls: [oversized],
      });
      try {
        const imgFile = new File(["fake"], "enormous.jpg", { type: "image/jpeg" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.errorMessage).toBe("Could not prepare this image. Try a smaller image file.");
      } finally {
        teardown();
      }
    });

    it("rejects when toDataURL returns a non-image data URL", async () => {
      const teardown = installCompressionMocks({
        canvasDataUrls: ["data:text/plain;base64,broken"],
      });
      try {
        const imgFile = new File(["fake"], "weird.png", { type: "image/png" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.errorMessage).toBe("Could not prepare this image. Try a smaller image file.");
      } finally {
        teardown();
      }
    });

    it("handles a 1x1 pixel image without error", async () => {
      const teardown = installCompressionMocks({
        imageWidth: 1,
        imageHeight: 1,
        canvasDataUrls: ["data:image/jpeg;base64,TINY"],
      });
      try {
        const imgFile = new File(["fake"], "pixel.png", { type: "image/png" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.formControl.value).toBe("data:image/jpeg;base64,TINY");
        expect(component.errorMessage).toBe("");
      } finally {
        teardown();
      }
    });

    it("handles a very wide image (extreme aspect ratio)", async () => {
      const teardown = installCompressionMocks({
        imageWidth: 4000,
        imageHeight: 10,
        canvasDataUrls: ["data:image/jpeg;base64,WIDE"],
      });
      try {
        const imgFile = new File(["fake"], "banner.jpg", { type: "image/jpeg" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.formControl.value).toBe("data:image/jpeg;base64,WIDE");
        expect(component.errorMessage).toBe("");
      } finally {
        teardown();
      }
    });
  });

  // ── clearImage edge cases ─────────────────────────────────────────────

  describe("clearImage edge cases", () => {
    it("does not update model when key is not a string", () => {
      const model: Record<string, unknown> = { someKey: "value" };
      component.field = {
        props: {},
        formControl: component.formControl,
        key: 42 as any,
        model,
      } as any;
      component.formControl.setValue("data:image/jpeg;base64,AAA");

      const input = document.createElement("input");
      component.clearImage(input);

      expect(component.formControl.value).toBe("");
      expect(model[42 as any]).toBeUndefined();
    });

    it("marks formControl dirty and touched", () => {
      const input = document.createElement("input");
      component.clearImage(input);
      expect(component.formControl.dirty).toBe(true);
      expect(component.formControl.touched).toBe(true);
    });
  });

  // ── Consecutive uploads ───────────────────────────────────────────────

  describe("consecutive uploads", () => {
    it("replaces previous upload value with new upload", async () => {
      const teardown = installCompressionMocks({
        canvasDataUrls: ["data:image/jpeg;base64,FIRST"],
      });
      try {
        const firstFile = new File(["a"], "first.jpg", { type: "image/jpeg" });
        const input1 = makeFileInput(firstFile);
        await component.onFileSelected({ target: input1 } as unknown as Event);
        expect(component.formControl.value).toBe("data:image/jpeg;base64,FIRST");
        expect(component.fileName).toBe("first.jpg");
      } finally {
        teardown();
      }

      const teardown2 = installCompressionMocks({
        canvasDataUrls: ["data:image/jpeg;base64,SECOND"],
      });
      try {
        const secondFile = new File(["b"], "second.png", { type: "image/png" });
        const input2 = makeFileInput(secondFile);
        await component.onFileSelected({ target: input2 } as unknown as Event);
        expect(component.formControl.value).toBe("data:image/jpeg;base64,SECOND");
        expect(component.fileName).toBe("second.png");
      } finally {
        teardown2();
      }
    });
  });

  // ── onFileSelected model update ───────────────────────────────────────

  describe("onFileSelected model update", () => {
    it("does not update model when key is not a string", async () => {
      const model: Record<string, unknown> = {};
      component.field = {
        props: {},
        formControl: component.formControl,
        key: 123 as any,
        model,
      } as any;

      const teardown = installCompressionMocks();
      try {
        const imgFile = new File(["fake"], "pic.png", { type: "image/png" });
        const input = makeFileInput(imgFile);
        await component.onFileSelected({ target: input } as unknown as Event);

        // formControl should be updated but model should NOT have the key
        expect(component.formControl.value).toBe("data:image/jpeg;base64,SMALL");
        expect(model[123 as any]).toBeUndefined();
      } finally {
        teardown();
      }
    });
  });
});
