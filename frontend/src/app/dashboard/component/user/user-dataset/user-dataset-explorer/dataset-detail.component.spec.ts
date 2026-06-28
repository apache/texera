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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { RouterTestingModule } from "@angular/router/testing";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { NzModalService } from "ng-zorro-antd/modal";
import { throwError, EMPTY } from "rxjs";
import type { Mocked } from "vitest";

import { DatasetDetailComponent } from "./dataset-detail.component";
import { DriveService } from "../../../../service/user/google-drive/drive.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { StubUserService } from "../../../../../common/service/user/stub-user.service";
import { commonTestProviders } from "../../../../../common/testing/test-utils";
import { DatasetVersion } from "../../../../../common/type/dataset";
import { MarkdownService } from "ngx-markdown";

// NzResizableDirective requires ResizeObserver which is absent in the test environment
global.ResizeObserver = class {
  observe() {}
  unobserve() {}
  disconnect() {}
};

describe("DatasetDetailComponent — Drive export", () => {
  let component: DatasetDetailComponent;
  let fixture: ComponentFixture<DatasetDetailComponent>;
  let driveServiceMock: Mocked<DriveService>;
  let notificationServiceMock: Mocked<NotificationService>;
  let downloadServiceMock: Mocked<DownloadService>;

  const stubVersion: DatasetVersion = { dvid: 1, name: "v1", creationTime: 0 } as unknown as DatasetVersion;

  beforeEach(async () => {
    driveServiceMock = { connect: vi.fn().mockReturnValue(EMPTY) } as unknown as Mocked<DriveService>;
    notificationServiceMock = { success: vi.fn(), error: vi.fn() } as unknown as Mocked<NotificationService>;
    downloadServiceMock = {
      downloadDatasetVersion: vi.fn().mockReturnValue(EMPTY),
      downloadSingleFile: vi.fn().mockReturnValue(EMPTY),
    } as unknown as Mocked<DownloadService>;

    await TestBed.configureTestingModule({
      imports: [DatasetDetailComponent, HttpClientTestingModule, RouterTestingModule, BrowserAnimationsModule],
      providers: [
        NzModalService,
        { provide: UserService, useClass: StubUserService },
        { provide: DriveService, useValue: driveServiceMock },
        { provide: NotificationService, useValue: notificationServiceMock },
        { provide: DownloadService, useValue: downloadServiceMock },
        { provide: MarkdownService, useValue: { parse: vi.fn(), render: vi.fn() } },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(DatasetDetailComponent);
    component = fixture.componentInstance;
    component.did = 1;
    component.selectedVersion = stubVersion;
    fixture.detectChanges();
  });

  describe("onClickDriveExportVersion", () => {
    it("calls driveService.connect()", () => {
      component.onClickDriveExportVersion();
      expect(driveServiceMock.connect).toHaveBeenCalled();
    });

    it("shows error notification when connect fails", () => {
      driveServiceMock.connect.mockReturnValue(throwError(() => new Error("blocked")));

      component.onClickDriveExportVersion();

      expect(notificationServiceMock.error).toHaveBeenCalledWith("Failed to connect to Google Drive");
    });
  });

  describe("onClickDriveExportFile", () => {
    it("calls driveService.connect()", () => {
      component.onClickDriveExportFile();
      expect(driveServiceMock.connect).toHaveBeenCalled();
    });

    it("shows error notification when connect fails", () => {
      driveServiceMock.connect.mockReturnValue(throwError(() => new Error("blocked")));

      component.onClickDriveExportFile();

      expect(notificationServiceMock.error).toHaveBeenCalledWith("Failed to connect to Google Drive");
    });
  });
});
