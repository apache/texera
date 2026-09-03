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
import { saveAs } from "file-saver";
import { FileSaverService } from "./file-saver.service";

// Replace the file-saver module wholesale (same precedent as menu.component.spec.ts);
// the service is a thin passthrough, so we assert it forwards to saveAs unchanged.
vi.mock("file-saver", () => ({ saveAs: vi.fn() }));

describe("FileSaverService", () => {
  let service: FileSaverService;

  beforeEach(() => {
    vi.mocked(saveAs).mockClear();
    TestBed.configureTestingModule({ providers: [FileSaverService] });
    service = TestBed.inject(FileSaverService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("delegates a Blob payload to saveAs with filename and options", () => {
    const blob = new Blob(["hello"], { type: "text/plain" });
    const options = { autoBom: true };

    service.saveAs(blob, "greeting.txt", options);

    expect(saveAs).toHaveBeenCalledTimes(1);
    expect(saveAs).toHaveBeenCalledWith(blob, "greeting.txt", options);
  });

  it("forwards a string payload (e.g. a URL) unchanged", () => {
    service.saveAs("https://example.com/file.csv", "file.csv");

    expect(saveAs).toHaveBeenCalledWith("https://example.com/file.csv", "file.csv", undefined);
  });

  it("propagates errors thrown by saveAs", () => {
    const err = new Error("boom");
    vi.mocked(saveAs).mockImplementationOnce(() => {
      throw err;
    });

    expect(() => service.saveAs(new Blob([]), "x.txt")).toThrow(err);
  });

  it("passes undefined through when filename and options are omitted", () => {
    const blob = new Blob([]);

    service.saveAs(blob);

    expect(saveAs).toHaveBeenCalledWith(blob, undefined, undefined);
  });

  it("propagates errors thrown by saveAs instead of swallowing them", () => {
    const failure = new Error("save failed");
    vi.mocked(saveAs).mockImplementationOnce(() => {
      throw failure;
    });

    expect(() => service.saveAs(new Blob(["oops"]), "oops.txt")).toThrow(failure);
  });
});
