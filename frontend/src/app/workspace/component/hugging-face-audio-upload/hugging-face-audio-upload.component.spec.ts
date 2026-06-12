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
import { HuggingFaceAudioUploadComponent } from "./hugging-face-audio-upload.component";

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

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HuggingFaceAudioUploadComponent, HttpClientTestingModule],
    }).compileComponents();

    const fixture = TestBed.createComponent(HuggingFaceAudioUploadComponent);
    component = fixture.componentInstance;
    formControl = new FormControl("");
    component.field = { formControl } as FieldTypeConfig;
    httpTestingController = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTestingController.verify();
  });

  it("should be defined", () => {
    expect(component).toBeDefined();
  });

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
});
