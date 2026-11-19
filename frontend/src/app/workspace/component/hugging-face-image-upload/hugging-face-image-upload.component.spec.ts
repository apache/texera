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

  async function selectImage(
    name: string,
    type: string,
    opts?: CompressionMockOptions,
    content = "fake"
  ): Promise<HTMLInputElement> {
    const teardown = installCompressionMocks(opts);
    try {
      const input = makeFileInput(new File([content], name, { type }));
      await component.onFileSelected({ target: input } as unknown as Event);
      return input;
    } finally {
      teardown();
    }
  }

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("derived view state", () => {
    it("reports no image when formControl is empty", () => {
      expect(component.hasMedia).toBe(false);
      expect(component.previewSrc).toBe("");
      expect(component.displayFileName).toBe("");
    });

    it("reports an image when formControl holds a data URL", () => {
      component.formControl.setValue("data:image/jpeg;base64,AAA");
      expect(component.hasMedia).toBe(true);
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
    it("clears prior error and returns early when no file is provided", async () => {
      component.errorMessage = "previous error";
      const input = makeFileInput();
      await component.onFileSelected({ target: input } as unknown as Event);
      expect(component.errorMessage).toBe("");
      expect(component.formControl.value).toBe("");
    });

    it("rejects non-image files and resets the input", async () => {
      await selectImage("note.txt", "text/plain", undefined, "hi");
      expect(component.errorMessage).toBe("Choose an image file.");
      expect(component.hasMedia).toBe(false);
    });

    it("clears a previously selected image when a non-image file is rejected", async () => {
      component.formControl.setValue("data:image/jpeg;base64,AAA");
      component.fileName = "old.jpg";
      await selectImage("note.txt", "text/plain", undefined, "hi");
      expect(component.errorMessage).toBe("Choose an image file.");
      expect(component.formControl.value).toBe("");
      expect(component.fileName).toBe("");
      expect(component.hasMedia).toBe(false);
    });

    it("reports an error when image compression fails", async () => {
      // jsdom's Image never fires onload/onerror unless stubbed; simulate FileReader onerror
      // so compressImage rejects and exercises the catch branch.
      await selectImage("broken.png", "image/png", { readerError: true });
      expect(component.errorMessage).toBe("Could not prepare this image. Try a smaller image file.");
      expect(component.hasMedia).toBe(false);
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

  describe("hasMedia edge cases", () => {
    it.each<[string, string | null, boolean]>([
      ["returns false for a server path string", "/uploads/photo.png", false],
      ["returns false for null value", null, false],
      ["returns false for non-image data URL", "data:audio/wav;base64,AAA", false],
      ["returns true for data:image/png URL", "data:image/png;base64,AAA", true],
    ])("%s", (_name, value, expected) => {
      component.formControl.setValue(value);
      expect(component.hasMedia).toBe(expected);
    });
  });

  describe("previewSrc", () => {
    it.each<[string, string, string]>([
      ["returns data URL when formControl has image", "data:image/jpeg;base64,AAA", "data:image/jpeg;base64,AAA"],
      ["returns empty string when formControl has no image", "", ""],
      ["returns empty string for server path", "/uploads/photo.png", ""],
    ])("%s", (_name, value, expected) => {
      component.formControl.setValue(value);
      expect(component.previewSrc).toBe(expected);
    });
  });

  describe("displayFileName", () => {
    it.each<[string, string, string, string]>([
      ["returns empty string when no image and no fileName", "", "", ""],
      [
        "returns 'Uploaded image' when image present but no fileName",
        "data:image/jpeg;base64,AAA",
        "",
        "Uploaded image",
      ],
      ["returns fileName when set, even without image", "", "photo.jpg", "photo.jpg"],
    ])("%s", (_name, formValue, fileName, expected) => {
      component.formControl.setValue(formValue);
      component.fileName = fileName;
      expect(component.displayFileName).toBe(expected);
    });
  });

  describe("rendered preview label", () => {
    const previewLabel = () => fixture.nativeElement.querySelector(".hf-image-meta span") as HTMLElement | null;

    it("labels the preview 'Uploaded image' when an image has no filename", () => {
      component.formControl.setValue("data:image/jpeg;base64,AAA");
      fixture.detectChanges();

      expect(previewLabel()?.textContent?.trim()).toBe("Uploaded image");
    });

    it("labels the preview with the filename once one is known", () => {
      component.formControl.setValue("data:image/jpeg;base64,AAA");
      component.fileName = "cat.jpg";
      fixture.detectChanges();

      expect(previewLabel()?.textContent?.trim()).toBe("cat.jpg");
    });

    it("renders no preview at all without an image, so the label is never empty", () => {
      // The preview only renders while previewSrc is non-empty, which happens exactly
      // when hasMedia is true — and displayFileName is never empty in that case.
      component.formControl.setValue("");
      fixture.detectChanges();

      expect(previewLabel()).toBeNull();
    });
  });

  // ── Successful upload ─────────────────────────────────────────────────

  describe("successful upload", () => {
    it("sets fileName, formControl, model, and marks dirty/touched", async () => {
      await selectImage("sunset.jpg", "image/jpeg");

      expect(component.fileName).toBe("sunset.jpg");
      expect(component.formControl.value).toBe("data:image/jpeg;base64,SMALL");
      expect(component.hasMedia).toBe(true);
      expect(component.formControl.dirty).toBe(true);
      expect(component.formControl.touched).toBe(true);
      expect((component.model as any).image).toBe("data:image/jpeg;base64,SMALL");
      expect(component.errorMessage).toBe("");
    });

    it("clears a previous error on successful upload", async () => {
      component.errorMessage = "previous failure";
      await selectImage("ok.png", "image/png");
      expect(component.errorMessage).toBe("");
    });
  });

  // ── compressImage rejection paths ─────────────────────────────────────

  describe("compressImage rejection paths", () => {
    it.each<[string, string, string, CompressionMockOptions]>([
      [
        "rejects when FileReader.result is not a string (ArrayBuffer)",
        "pic.png",
        "image/png",
        { readerResult: new ArrayBuffer(8) },
      ],
      ["rejects when FileReader.result is null", "pic.png", "image/png", { readerResult: null }],
      ["rejects when Image fires onerror", "corrupt.jpg", "image/jpeg", { imageError: true }],
    ])("%s", async (_name, filename, type, opts) => {
      await selectImage(filename, type, opts);
      expect(component.errorMessage).toBe("Could not prepare this image. Try a smaller image file.");
      expect(component.hasMedia).toBe(false);
    });
  });

  // ── renderCompressedDataUrl edge cases ────────────────────────────────

  describe("renderCompressedDataUrl (via onFileSelected)", () => {
    const oversized = "data:image/jpeg;base64," + "A".repeat(50000);

    it.each<[string, string, string, CompressionMockOptions]>([
      // bestDataUrl starts as "" → compressed is "" → fails startsWith check → rejects
      ["returns empty when canvas context is null", "pic.png", "image/png", { nullCanvasContext: true }],
      [
        "rejects when compressed result never fits within size limit",
        "enormous.jpg",
        "image/jpeg",
        { canvasDataUrls: [oversized] },
      ],
      [
        "rejects when toDataURL returns a non-image data URL",
        "weird.png",
        "image/png",
        { canvasDataUrls: ["data:text/plain;base64,broken"] },
      ],
    ])("%s", async (_name, filename, type, opts) => {
      await selectImage(filename, type, opts);
      expect(component.errorMessage).toBe("Could not prepare this image. Try a smaller image file.");
    });

    it.each<[string, string, string, CompressionMockOptions, string]>([
      [
        "accepts a data URL that fits within the size limit on first attempt",
        "small.jpg",
        "image/jpeg",
        { canvasDataUrls: ["data:image/jpeg;base64,FIT"] },
        "data:image/jpeg;base64,FIT",
      ],
      [
        "reduces quality when the first toDataURL result exceeds the size limit",
        "large.jpg",
        "image/jpeg",
        { canvasDataUrls: [oversized, "data:image/jpeg;base64,OK"] },
        "data:image/jpeg;base64,OK",
      ],
      [
        // Quality has 5 steps (0.75, 0.65, 0.55, 0.45, 0.35).
        // After exhausting quality at the first maxDimension, it shrinks dimension and retries.
        // We make the first 5 calls oversized (quality loop at dim=512), then return small on the 6th (dim=384).
        "reduces dimension when quality loop alone is not enough",
        "huge.jpg",
        "image/jpeg",
        { canvasDataUrls: [oversized, oversized, oversized, oversized, oversized, "data:image/jpeg;base64,SHRUNK"] },
        "data:image/jpeg;base64,SHRUNK",
      ],
      [
        "handles a 1x1 pixel image without error",
        "pixel.png",
        "image/png",
        { imageWidth: 1, imageHeight: 1, canvasDataUrls: ["data:image/jpeg;base64,TINY"] },
        "data:image/jpeg;base64,TINY",
      ],
      [
        "handles a very wide image (extreme aspect ratio)",
        "banner.jpg",
        "image/jpeg",
        { imageWidth: 4000, imageHeight: 10, canvasDataUrls: ["data:image/jpeg;base64,WIDE"] },
        "data:image/jpeg;base64,WIDE",
      ],
    ])("%s", async (_name, filename, type, opts, expectedValue) => {
      await selectImage(filename, type, opts);
      expect(component.formControl.value).toBe(expectedValue);
      expect(component.errorMessage).toBe("");
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
      await selectImage("first.jpg", "image/jpeg", { canvasDataUrls: ["data:image/jpeg;base64,FIRST"] }, "a");
      expect(component.formControl.value).toBe("data:image/jpeg;base64,FIRST");
      expect(component.fileName).toBe("first.jpg");

      await selectImage("second.png", "image/png", { canvasDataUrls: ["data:image/jpeg;base64,SECOND"] }, "b");
      expect(component.formControl.value).toBe("data:image/jpeg;base64,SECOND");
      expect(component.fileName).toBe("second.png");
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

      await selectImage("pic.png", "image/png");

      // formControl should be updated but model should NOT have the key
      expect(component.formControl.value).toBe("data:image/jpeg;base64,SMALL");
      expect(model[123 as any]).toBeUndefined();
    });
  });

  // ── Rendered template ──

  describe("template rendering", () => {
    const DATA_URL = "data:image/jpeg;base64,AAA";

    const preview = () => fixture.debugElement.query(By.css(".hf-image-preview"));

    /** Put the component in the "image selected" state and render it. */
    function renderWithImage(fileName = ""): void {
      component.fileName = fileName;
      component.formControl.setValue(DATA_URL);
      fixture.detectChanges();
    }

    it("renders no preview panel while no image is selected", () => {
      expect(preview()).toBeNull();
    });

    it("renders the preview image bound to the stored data URL", () => {
      renderWithImage();

      expect(preview()).toBeTruthy();
      const img = fixture.debugElement.query(By.css(".hf-image-preview img"));
      expect(img.nativeElement.getAttribute("src")).toBe(DATA_URL);
      expect(img.nativeElement.getAttribute("alt")).toBe("Uploaded Hugging Face task input");
    });

    it("labels the preview with the default text when no file name is known", () => {
      renderWithImage();

      expect(fixture.debugElement.query(By.css(".hf-image-meta span")).nativeElement.textContent.trim()).toBe(
        "Uploaded image"
      );
    });

    it("labels the preview with the selected file name once one is known", () => {
      renderWithImage("cat.png");

      expect(fixture.debugElement.query(By.css(".hf-image-meta span")).nativeElement.textContent.trim()).toBe(
        "cat.png"
      );
    });

    it("the Clear button clears the image and removes the preview panel", () => {
      renderWithImage("cat.png");

      fixture.debugElement.query(By.css(".hf-image-meta button")).triggerEventHandler("click", null);
      fixture.detectChanges();

      expect(component.formControl.value).toBe("");
      expect(component.fileName).toBe("");
      expect(preview()).toBeNull();
    });

    it("renders the error message when one is set", () => {
      component.errorMessage = "Choose an image file.";
      fixture.detectChanges();

      const error = fixture.debugElement.query(By.css(".hf-image-error"));
      expect(error).toBeTruthy();
      expect(error.nativeElement.textContent.trim()).toBe("Choose an image file.");
    });

    it("a change event with no file selected leaves the control untouched", () => {
      const input = fixture.debugElement.query(By.css("input[type='file']"));

      input.nativeElement.dispatchEvent(new Event("change"));
      fixture.detectChanges();

      expect(component.formControl.value).toBe("");
      expect(component.errorMessage).toBe("");
      expect(preview()).toBeNull();
    });
  });
});
