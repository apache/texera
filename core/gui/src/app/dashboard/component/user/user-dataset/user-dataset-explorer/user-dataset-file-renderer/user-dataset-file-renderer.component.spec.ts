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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { UserDatasetFileRendererComponent } from "./user-dataset-file-renderer.component";
import { DatasetService } from "../../../../../service/user/dataset/dataset.service";
import { NotificationService } from "../../../../../../common/service/notification/notification.service";
import { DomSanitizer } from "@angular/platform-browser";

describe("UserDatasetFileRendererComponent", () => {
  let component: UserDatasetFileRendererComponent;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      declarations: [UserDatasetFileRendererComponent],
      providers: [
        DatasetService,
        NotificationService,
        { provide: DomSanitizer, useValue: jasmine.createSpyObj("DomSanitizer", ["bypassSecurityTrustUrl"]) },
      ],
    });
    const fixture = TestBed.createComponent(UserDatasetFileRendererComponent);
    component = fixture.componentInstance;
  });

  it("should return true for supported MIME type", () => {
    const supportedMimeType = "image/jpeg"; // Example of a supported MIME type
    const result = component.isPreviewSupported(supportedMimeType);
    expect(result).toBeTrue();
  });

  it("should return false for unsupported MIME type", () => {
    const unsupportedMimeType = "application/unknown"; // Example of an unsupported MIME type
    const result = component.isPreviewSupported(unsupportedMimeType);
    expect(result).toBeFalse();
  });
});
