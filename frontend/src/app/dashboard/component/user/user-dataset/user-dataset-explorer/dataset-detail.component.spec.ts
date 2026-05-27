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
import { NO_ERRORS_SCHEMA } from "@angular/core";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { RouterTestingModule } from "@angular/router/testing";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { of, Subject } from "rxjs";

import { DatasetDetailComponent } from "./dataset-detail.component";
import { DatasetService } from "../../../../service/user/dataset/dataset.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { DriveService } from "../../../../service/user/google-drive/drive.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { StubUserService } from "../../../../../common/service/user/stub-user.service";
import { commonTestProviders } from "../../../../../common/testing/test-utils";
import { ActivatedRoute } from "@angular/router";
import { HubService } from "../../../../../hub/service/hub.service";
import { AdminSettingsService } from "../../../../service/admin/settings/admin-settings.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { NzModalService } from "ng-zorro-antd/modal";

describe("DatasetDetailComponent - Drive integration", () => {
  let component: DatasetDetailComponent;
  let fixture: ComponentFixture<DatasetDetailComponent>;
  let driveServiceMock: {
    getToken: ReturnType<typeof vi.fn>;
    onConnected: ReturnType<typeof vi.fn>;
    connect: ReturnType<typeof vi.fn>;
    exportToDrive: ReturnType<typeof vi.fn>;
  };
  let datasetServiceMock: Record<string, ReturnType<typeof vi.fn>>;
  let notificationServiceMock: { success: ReturnType<typeof vi.fn>; error: ReturnType<typeof vi.fn> };
  let connected$: Subject<void>;

  beforeEach(async () => {
    connected$ = new Subject<void>();

    driveServiceMock = {
      getToken: vi.fn().mockReturnValue(of({ status: "ok", accessToken: "token" })),
      onConnected: vi.fn().mockReturnValue(connected$.asObservable()),
      connect: vi.fn(),
      exportToDrive: vi.fn().mockReturnValue(of(undefined)),
    };

    datasetServiceMock = {
      retrieveDatasetVersionZip: vi.fn().mockReturnValue(of(new Blob(["zip"], { type: "application/zip" }))),
      retrieveDatasetVersionSingleFile: vi.fn().mockReturnValue(of(new Blob(["file"]))),
      getDataset: vi.fn().mockReturnValue(
        of({
          dataset: { name: "test", description: "", isPublic: false, isDownloadable: true, creationTime: undefined },
          accessPrivilege: "WRITE",
          ownerEmail: "test@test.com",
          isOwner: true,
        })
      ),
      retrieveDatasetVersionList: vi.fn().mockReturnValue(of([])),
    };

    notificationServiceMock = { success: vi.fn(), error: vi.fn() };

    TestBed.overrideComponent(DatasetDetailComponent, { set: { template: "" } });

    await TestBed.configureTestingModule({
      imports: [DatasetDetailComponent, HttpClientTestingModule, BrowserAnimationsModule, RouterTestingModule],
      providers: [
        { provide: UserService, useClass: StubUserService },
        { provide: DriveService, useValue: driveServiceMock },
        { provide: DatasetService, useValue: datasetServiceMock },
        { provide: NotificationService, useValue: notificationServiceMock },
        {
          provide: ActivatedRoute,
          useValue: { params: of({ did: 1 }), data: of({}) },
        },
        {
          provide: HubService,
          useValue: {
            getCounts: vi.fn().mockReturnValue(of([])),
            postView: vi.fn().mockReturnValue(of(0)),
            isLiked: vi.fn().mockReturnValue(of([])),
          },
        },
        {
          provide: AdminSettingsService,
          useValue: { getSetting: vi.fn().mockReturnValue(of("10")) },
        },
        {
          provide: DownloadService,
          useValue: {
            downloadDatasetVersion: vi.fn().mockReturnValue(of(undefined)),
            downloadDatasetFile: vi.fn().mockReturnValue(of(undefined)),
          },
        },
        NzModalService,
        ...commonTestProviders,
      ],
      schemas: [NO_ERRORS_SCHEMA],
    }).compileComponents();

    fixture = TestBed.createComponent(DatasetDetailComponent);
    component = fixture.componentInstance;
    component.currentUid = 1;
    fixture.detectChanges();
  });

  it("should set isDriveConnected = true when token status is ok", () => {
    expect(component.isDriveConnected).toBe(true);
  });

  it("should set isDriveConnected = false when getToken returns error status", () => {
    driveServiceMock.getToken.mockReturnValue(of({ status: "error", accessToken: "" }));
    const newFixture = TestBed.createComponent(DatasetDetailComponent);
    const newComponent = newFixture.componentInstance;
    newComponent.currentUid = 1;
    newComponent.ngOnInit();
    expect(newComponent.isDriveConnected).toBe(false);
  });

  it("should set isDriveConnected = true and show toast when onConnected fires", () => {
    // Drive subscription is already active from beforeEach fixture.detectChanges()
    component.isDriveConnected = false;

    connected$.next();

    expect(component.isDriveConnected).toBe(true);
    expect(notificationServiceMock.success).toHaveBeenCalledWith("Google Drive connected");
  });

  describe("onClickDriveExportVersion", () => {
    it("should call connect() when not connected", () => {
      component.isDriveConnected = false;

      component.onClickDriveExportVersion();

      expect(driveServiceMock.connect).toHaveBeenCalled();
      expect(driveServiceMock.exportToDrive).not.toHaveBeenCalled();
    });

    it("should do nothing when did or selectedVersion is missing", () => {
      component.isDriveConnected = true;
      component.did = undefined;

      component.onClickDriveExportVersion();

      expect(driveServiceMock.exportToDrive).not.toHaveBeenCalled();
    });

    it("should export version zip to Drive and show success toast", () => {
      component.isDriveConnected = true;
      component.did = 42;
      component.selectedVersion = { dvid: 7, name: "v1" } as any;
      component.datasetName = "my-dataset";

      component.onClickDriveExportVersion();

      expect(datasetServiceMock.retrieveDatasetVersionZip).toHaveBeenCalledWith(42, 7);
      expect(driveServiceMock.exportToDrive).toHaveBeenCalledWith(expect.any(Blob), "my-dataset-v1.zip");
      expect(notificationServiceMock.success).toHaveBeenCalledWith("Exported to Google Drive");
    });
  });

  describe("onClickDriveExportFile", () => {
    it("should call connect() when not connected", () => {
      component.isDriveConnected = false;

      component.onClickDriveExportFile();

      expect(driveServiceMock.connect).toHaveBeenCalled();
      expect(driveServiceMock.exportToDrive).not.toHaveBeenCalled();
    });

    it("should do nothing when no file is selected", () => {
      component.isDriveConnected = true;
      component.currentDisplayedFileName = "";

      component.onClickDriveExportFile();

      expect(driveServiceMock.exportToDrive).not.toHaveBeenCalled();
    });

    it("should export the selected file to Drive and show success toast", () => {
      component.isDriveConnected = true;
      component.currentDisplayedFileName = "path/to/report.csv";
      component.datasetIsPublic = false;
      component.isOwner = true;

      component.onClickDriveExportFile();

      expect(datasetServiceMock.retrieveDatasetVersionSingleFile).toHaveBeenCalledWith("path/to/report.csv", true);
      expect(driveServiceMock.exportToDrive).toHaveBeenCalledWith(expect.any(Blob), "report.csv");
      expect(notificationServiceMock.success).toHaveBeenCalledWith("Exported to Google Drive");
    });
  });
});
