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
import { By } from "@angular/platform-browser";
import { HuggingFaceAudioUploadComponent } from "./hugging-face-audio-upload.component";
import { commonTestProviders } from "../../../common/testing/test-utils";

const MAX_AUDIO_BYTES = 25 * 1024 * 1024;

describe("HuggingFaceAudioUploadComponent", () => {
  let component: HuggingFaceAudioUploadComponent;
  let fixture: ComponentFixture<HuggingFaceAudioUploadComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HuggingFaceAudioUploadComponent],
      providers: [...commonTestProviders],
    }).compileComponents();

    fixture = TestBed.createComponent(HuggingFaceAudioUploadComponent);
    component = fixture.componentInstance;
    component.field = {
      props: {},
      formControl: new FormControl(""),
      key: "audioInput",
      model: {},
    } as any;
    fixture.detectChanges();
  });

  // ── Helpers ──────────────────────────────────────────────────────────────

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

  function sizedFile(name: string, type: string, size: number, content = "audio"): File {
    const file = new File([content], name, { type });
    Object.defineProperty(file, "size", { value: size, configurable: true });
    return file;
  }

  interface ReaderMockOptions {
    /** Value returned by FileReader.result. Default: a valid audio data URL. */
    readerResult?: string | ArrayBuffer | null;
    /** If true, FileReader fires onerror instead of onload. */
    readerError?: boolean;
  }

  /** Installs a fake FileReader that resolves via microtask. Returns teardown. */
  function installReaderMock(opts: ReaderMockOptions = {}): () => void {
    const savedFileReader = globalThis.FileReader;
    const readerResult = "readerResult" in opts ? opts.readerResult : "data:audio/wav;base64,AAAA";

    class FakeFileReader {
      onload: ((e: Event) => void) | null = null;
      onerror: ((e: Event) => void) | null = null;
      error: unknown = null;
      result: string | ArrayBuffer | null = readerResult!;
      readAsDataURL() {
        queueMicrotask(() => {
          if (opts.readerError) {
            this.error = new Error("read failed");
            this.onerror?.(new Event("error"));
          } else {
            this.onload?.(new Event("load"));
          }
        });
      }
    }
    (globalThis as any).FileReader = FakeFileReader;

    return () => {
      (globalThis as any).FileReader = savedFileReader;
    };
  }

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  // ── Derived view state ─────────────────────────────────────────────────

  describe("derived view state", () => {
    it("reports no audio when formControl is empty", () => {
      expect(component.hasAudio).toBe(false);
      expect(component.previewSrc).toBe("");
      expect(component.displayFileName).toBe("");
    });

    it("reports audio when formControl holds a data:audio URL", () => {
      component.formControl.setValue("data:audio/wav;base64,AAA");
      expect(component.hasAudio).toBe(true);
      expect(component.previewSrc).toBe("data:audio/wav;base64,AAA");
      expect(component.displayFileName).toBe("Selected audio");
    });

    it("prefers the explicit filename over the fallback label", () => {
      component.formControl.setValue("data:audio/wav;base64,AAA");
      component.fileName = "clip.wav";
      expect(component.displayFileName).toBe("clip.wav");
    });

    it("returns false for a leftover server path (legacy saved value)", () => {
      component.formControl.setValue("/uploads/clip.wav");
      expect(component.hasAudio).toBe(false);
      expect(component.previewSrc).toBe("");
    });

    it("returns false for a non-audio data URL", () => {
      component.formControl.setValue("data:image/png;base64,AAA");
      expect(component.hasAudio).toBe(false);
    });

    it("returns false for a null value", () => {
      component.formControl.setValue(null);
      expect(component.hasAudio).toBe(false);
    });
  });

  // ── onFileSelected ──────────────────────────────────────────────────────

  describe("onFileSelected", () => {
    it("clears prior error and returns early when no file is provided", async () => {
      component.errorMessage = "previous error";
      const input = makeFileInput();
      await component.onFileSelected({ target: input } as unknown as Event);
      expect(component.errorMessage).toBe("");
      expect(component.formControl.value).toBe("");
    });

    it("rejects a non-audio file and resets the input", async () => {
      const file = sizedFile("doc.pdf", "application/pdf", 100);
      const input = makeFileInput(file);
      await component.onFileSelected({ target: input } as unknown as Event);
      expect(component.errorMessage).toBe("Choose an audio file.");
      expect(component.hasAudio).toBe(false);
      expect(input.value).toBe("");
    });

    it("rejects a file over the size cap", async () => {
      const file = sizedFile("big.wav", "audio/wav", MAX_AUDIO_BYTES + 1);
      const input = makeFileInput(file);
      await component.onFileSelected({ target: input } as unknown as Event);
      expect(component.errorMessage).toBe("Audio file is too large (max 25 MB).");
      expect(component.formControl.value).toBe("");
    });

    it("accepts a file exactly at the size cap", async () => {
      const teardown = installReaderMock();
      try {
        const file = sizedFile("edge.wav", "audio/wav", MAX_AUDIO_BYTES);
        const input = makeFileInput(file);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.errorMessage).toBe("");
        expect(component.formControl.value).toBe("data:audio/wav;base64,AAAA");
      } finally {
        teardown();
      }
    });

    it("reads an audio file into a data URL and sets form + model state", async () => {
      const teardown = installReaderMock();
      try {
        const file = sizedFile("clip.wav", "audio/wav", 2048);
        const input = makeFileInput(file);
        await component.onFileSelected({ target: input } as unknown as Event);

        expect(component.formControl.value).toBe("data:audio/wav;base64,AAAA");
        expect(component.fileName).toBe("clip.wav");
        expect(component.hasAudio).toBe(true);
        expect(component.formControl.dirty).toBe(true);
        expect(component.formControl.touched).toBe(true);
        expect((component.model as any).audioInput).toBe("data:audio/wav;base64,AAAA");
        expect(component.errorMessage).toBe("");
      } finally {
        teardown();
      }
    });

    it("clears a previous error on a successful read", async () => {
      component.errorMessage = "previous failure";
      const teardown = installReaderMock();
      try {
        const file = sizedFile("ok.mp3", "audio/mpeg", 2048);
        const input = makeFileInput(file);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.errorMessage).toBe("");
      } finally {
        teardown();
      }
    });

    it("rejects when FileReader yields a non-audio data URL", async () => {
      const teardown = installReaderMock({ readerResult: "data:text/plain;base64,AAAA" });
      try {
        const file = sizedFile("weird.wav", "audio/wav", 2048);
        const input = makeFileInput(file);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.errorMessage).toBe("Could not read this audio file.");
        expect(component.hasAudio).toBe(false);
      } finally {
        teardown();
      }
    });

    it("rejects when FileReader.result is not a string (ArrayBuffer)", async () => {
      const teardown = installReaderMock({ readerResult: new ArrayBuffer(8) });
      try {
        const file = sizedFile("clip.wav", "audio/wav", 2048);
        const input = makeFileInput(file);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.errorMessage).toBe("Could not read this audio file.");
      } finally {
        teardown();
      }
    });

    it("rejects when FileReader fires onerror", async () => {
      const teardown = installReaderMock({ readerError: true });
      try {
        const file = sizedFile("broken.wav", "audio/wav", 2048);
        const input = makeFileInput(file);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.errorMessage).toBe("Could not read this audio file.");
        expect(component.hasAudio).toBe(false);
      } finally {
        teardown();
      }
    });

    it("does not update the model when key is not a string", async () => {
      const model: Record<string, unknown> = {};
      component.field = {
        props: {},
        formControl: component.formControl,
        key: 42 as any,
        model,
      } as any;

      const teardown = installReaderMock();
      try {
        const file = sizedFile("clip.wav", "audio/wav", 2048);
        const input = makeFileInput(file);
        await component.onFileSelected({ target: input } as unknown as Event);
        expect(component.formControl.value).toBe("data:audio/wav;base64,AAAA");
        expect(model[42 as any]).toBeUndefined();
      } finally {
        teardown();
      }
    });

    it("replaces a previous upload value with a new one", async () => {
      const teardown = installReaderMock({ readerResult: "data:audio/wav;base64,FIRST" });
      try {
        const input1 = makeFileInput(sizedFile("first.wav", "audio/wav", 2048));
        await component.onFileSelected({ target: input1 } as unknown as Event);
        expect(component.formControl.value).toBe("data:audio/wav;base64,FIRST");
        expect(component.fileName).toBe("first.wav");
      } finally {
        teardown();
      }

      const teardown2 = installReaderMock({ readerResult: "data:audio/mpeg;base64,SECOND" });
      try {
        const input2 = makeFileInput(sizedFile("second.mp3", "audio/mpeg", 2048));
        await component.onFileSelected({ target: input2 } as unknown as Event);
        expect(component.formControl.value).toBe("data:audio/mpeg;base64,SECOND");
        expect(component.fileName).toBe("second.mp3");
      } finally {
        teardown2();
      }
    });
  });

  // ── clearAudio ──────────────────────────────────────────────────────────

  describe("clearAudio", () => {
    it("resets file state, the form control, and any model value", () => {
      (component.field as any).model = { audioInput: "data:audio/wav;base64,AAA" };
      component.formControl.setValue("data:audio/wav;base64,AAA");
      component.fileName = "clip.wav";
      component.errorMessage = "some error";

      const input = document.createElement("input");
      input.type = "file";
      component.clearAudio(input);

      expect(component.fileName).toBe("");
      expect(component.errorMessage).toBe("");
      expect(input.value).toBe("");
      expect(component.formControl.value).toBe("");
      expect(component.formControl.dirty).toBe(true);
      expect(component.formControl.touched).toBe(true);
      expect((component.model as any).audioInput).toBe("");
    });

    it("does not update the model when key is not a string", () => {
      const model: Record<string, unknown> = { someKey: "value" };
      component.field = {
        props: {},
        formControl: component.formControl,
        key: 42 as any,
        model,
      } as any;
      component.formControl.setValue("data:audio/wav;base64,AAA");

      const input = document.createElement("input");
      component.clearAudio(input);

      expect(component.formControl.value).toBe("");
      expect(model[42 as any]).toBeUndefined();
    });
  });

  // ── Template rendering ────────────────────────────────────────────────

  describe("template rendering", () => {
    it("renders the file input", () => {
      fixture.detectChanges();
      expect(fixture.debugElement.query(By.css("input[type='file']"))).toBeTruthy();
    });

    it("does not render the preview section when there is no audio", () => {
      fixture.detectChanges();
      expect(fixture.debugElement.query(By.css(".hf-audio-preview"))).toBeNull();
    });

    it("renders the preview and audio element for a data:audio value", () => {
      component.formControl.setValue("data:audio/wav;base64,AAA");
      fixture.detectChanges();
      expect(fixture.debugElement.query(By.css(".hf-audio-preview"))).toBeTruthy();
      const audio = fixture.debugElement.query(By.css("audio")).nativeElement as HTMLAudioElement;
      expect(audio.src).toContain("data:audio/wav");
    });

    it("shows the display filename in the preview meta", () => {
      component.formControl.setValue("data:audio/wav;base64,AAA");
      component.fileName = "clip.wav";
      fixture.detectChanges();
      const span = fixture.debugElement.query(By.css(".hf-audio-meta span"));
      expect((span.nativeElement as HTMLElement).textContent?.trim()).toBe("clip.wav");
    });

    it("falls back to 'Selected audio' when no filename is set", () => {
      component.formControl.setValue("data:audio/wav;base64,AAA");
      component.fileName = "";
      fixture.detectChanges();
      const span = fixture.debugElement.query(By.css(".hf-audio-meta span"));
      expect((span.nativeElement as HTMLElement).textContent?.trim()).toBe("Selected audio");
    });

    it("shows the error message when errorMessage is set", () => {
      component.errorMessage = "Could not read this audio file.";
      fixture.detectChanges();
      const errorEl = fixture.debugElement.query(By.css(".hf-audio-error"));
      expect(errorEl).toBeTruthy();
      expect((errorEl.nativeElement as HTMLElement).textContent?.trim()).toBe("Could not read this audio file.");
    });

    it("calls clearAudio when the Clear button is clicked", () => {
      const clearSpy = vi.spyOn(component, "clearAudio");
      component.formControl.setValue("data:audio/wav;base64,AAA");
      fixture.detectChanges();
      fixture.debugElement.query(By.css("button[nz-button]")).triggerEventHandler("click", null);
      expect(clearSpy).toHaveBeenCalled();
    });
  });
});
