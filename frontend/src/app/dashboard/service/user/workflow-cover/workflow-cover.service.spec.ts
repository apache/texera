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
import { WorkflowCoverService } from "./workflow-cover.service";
import { AppSettings } from "../../../../common/app-setting";

describe("WorkflowCoverService", () => {
  let service: WorkflowCoverService;
  let httpMock: HttpTestingController;
  const coverUrl = (wid: number) => `${AppSettings.getApiEndpoint()}/workflow/${wid}/cover`;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [WorkflowCoverService],
    });
    service = TestBed.inject(WorkflowCoverService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("getCover returns the stored image data URL", () => {
    let result: string | undefined;
    service.getCover(7).subscribe(image => (result = image));
    const req = httpMock.expectOne(coverUrl(7));
    expect(req.request.method).toBe("GET");
    req.flush({ image: "data:image/jpeg;base64,abc" });
    expect(result).toBe("data:image/jpeg;base64,abc");
  });

  it("getCover resolves to undefined when no cover exists (404)", () => {
    let result: string | undefined = "unset";
    service.getCover(7).subscribe(image => (result = image));
    httpMock.expectOne(coverUrl(7)).flush(null, { status: 404, statusText: "Not Found" });
    expect(result).toBeUndefined();
  });

  it("clearCover issues a DELETE", () => {
    let completed = false;
    service.clearCover(7).subscribe(() => (completed = true));
    const req = httpMock.expectOne(coverUrl(7));
    expect(req.request.method).toBe("DELETE");
    req.flush(null);
    expect(completed).toBe(true);
  });

  it("setCoverFromFile PUTs the resized data URL and resolves with it", async () => {
    const dataUrl = "data:image/jpeg;base64,resized";
    // The resize step relies on canvas/Image decoding, which jsdom cannot run;
    // stub it so the test exercises the upload wiring deterministically.
    (service as any).fileToResizedDataUrl = vi.fn().mockResolvedValue(dataUrl);
    const file = new File(["x"], "pic.png", { type: "image/png" });

    const resultPromise = service.setCoverFromFile(7, file);
    // Let the stubbed resize promise settle so the HTTP request is issued.
    await Promise.resolve();
    await Promise.resolve();

    const req = httpMock.expectOne(coverUrl(7));
    expect(req.request.method).toBe("PUT");
    expect(req.request.body).toEqual({ image: dataUrl });
    req.flush(null);

    await expect(resultPromise).resolves.toBe(dataUrl);
  });

  describe("fileToResizedDataUrl", () => {
    const realImage = (globalThis as any).Image;
    const realFileReader = globalThis.FileReader;
    const realCreateElement = document.createElement.bind(document);

    // jsdom never fires Image onload/onerror and cannot rasterize a canvas,
    // so stub Image and the canvas element to drive the resize deterministically.
    function stubImage(behavior: "load" | "error", width = 1280, height = 640): void {
      class FakeImage {
        onload: (() => void) | null = null;
        onerror: (() => void) | null = null;
        width = width;
        height = height;
        set src(_value: string) {
          queueMicrotask(() => (behavior === "load" ? this.onload?.() : this.onerror?.()));
        }
      }
      (globalThis as any).Image = FakeImage;
    }

    function stubCanvas(ctx: unknown, dataUrl = "data:image/jpeg;base64,RESIZED") {
      const canvas = {
        width: 0,
        height: 0,
        getContext: vi.fn().mockReturnValue(ctx),
        toDataURL: vi.fn().mockReturnValue(dataUrl),
      };
      vi.spyOn(document, "createElement").mockImplementation(((tag: string) =>
        tag === "canvas" ? canvas : realCreateElement(tag)) as any);
      return canvas;
    }

    const resize = (file: File): Promise<string> => (service as any).fileToResizedDataUrl(file);
    const imageFile = () => new File(["x"], "pic.png", { type: "image/png" });

    afterEach(() => {
      (globalThis as any).Image = realImage;
      (globalThis as any).FileReader = realFileReader;
      vi.restoreAllMocks();
    });

    it("downscales a large image along its longest edge and re-encodes it as jpeg", async () => {
      stubImage("load", 1280, 640);
      const canvas = stubCanvas({ drawImage: vi.fn() });

      await expect(resize(imageFile())).resolves.toBe("data:image/jpeg;base64,RESIZED");
      // Longest edge 1280 scales to the 640px cap, halving both dimensions.
      expect(canvas.width).toBe(640);
      expect(canvas.height).toBe(320);
      expect(canvas.toDataURL).toHaveBeenCalledWith("image/jpeg", 0.8);
    });

    it("leaves an already-small image at its natural size", async () => {
      stubImage("load", 100, 50);
      const canvas = stubCanvas({ drawImage: vi.fn() });

      await resize(imageFile());
      expect(canvas.width).toBe(100);
      expect(canvas.height).toBe(50);
    });

    it("rejects when a 2d canvas context is unavailable", async () => {
      stubImage("load");
      stubCanvas(null);
      await expect(resize(imageFile())).rejects.toThrow("Unable to process the selected image.");
    });

    it("rejects when the file is not a decodable image", async () => {
      stubImage("error");
      await expect(resize(imageFile())).rejects.toThrow("The selected file is not a valid image.");
    });

    it("rejects when the file cannot be read", async () => {
      class FailingFileReader {
        onload: ((e: Event) => void) | null = null;
        onerror: ((e: Event) => void) | null = null;
        readAsDataURL() {
          queueMicrotask(() => this.onerror?.(new Event("error")));
        }
      }
      (globalThis as any).FileReader = FailingFileReader;
      await expect(resize(imageFile())).rejects.toThrow("Failed to read the selected image.");
    });
  });
});
