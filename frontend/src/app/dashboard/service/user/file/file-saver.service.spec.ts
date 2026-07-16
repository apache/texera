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
import * as FileSaver from "file-saver";
import { FileSaverService } from "./file-saver.service";

vi.mock("file-saver", () => ({ saveAs: vi.fn() }));

describe("FileSaverService", () => {
  let service: FileSaverService;

  beforeEach(() => {
    vi.clearAllMocks();
    TestBed.configureTestingModule({ providers: [FileSaverService] });
    service = TestBed.inject(FileSaverService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("delegates a Blob payload to FileSaver.saveAs with filename and options", () => {
    const blob = new Blob(["hello"], { type: "text/plain" });
    const options: FileSaver.FileSaverOptions = { autoBom: true };

    service.saveAs(blob, "greeting.txt", options);

    expect(FileSaver.saveAs).toHaveBeenCalledTimes(1);
    expect(FileSaver.saveAs).toHaveBeenCalledWith(blob, "greeting.txt", options);
  });

  it("forwards a string payload (e.g. a URL) unchanged", () => {
    service.saveAs("https://example.com/file.csv", "file.csv");

    expect(FileSaver.saveAs).toHaveBeenCalledWith("https://example.com/file.csv", "file.csv", undefined);
  });

  it("passes undefined through when filename and options are omitted", () => {
    const blob = new Blob([]);

    service.saveAs(blob);

    expect(FileSaver.saveAs).toHaveBeenCalledWith(blob, undefined, undefined);
  });
});
