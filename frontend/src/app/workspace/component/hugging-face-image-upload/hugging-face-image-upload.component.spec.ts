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
});
