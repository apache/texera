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
import { GoogleDriveConnectComponent } from "./google-drive-connect.component";
import { DriveService } from "../../../service/user/google-drive/drive.service";
import { of, Subject, throwError } from "rxjs";
import { NO_ERRORS_SCHEMA } from "@angular/core";
import type { Mocked } from "vitest";

describe("GoogleDriveConnectComponent", () => {
  let component: GoogleDriveConnectComponent;
  let fixture: ComponentFixture<GoogleDriveConnectComponent>;
  let driveServiceMock: Mocked<DriveService>;
  let connected$: Subject<void>;

  beforeEach(async () => {
    connected$ = new Subject<void>();

    driveServiceMock = {
      getToken: vi.fn().mockReturnValue(of({ status: "ok", accessToken: "token123" })),
      onConnected: vi.fn().mockReturnValue(connected$.asObservable()),
      connect: vi.fn(),
      exportToDrive: vi.fn(),
      openFolderPicker: vi.fn(),
    } as unknown as Mocked<DriveService>;

    await TestBed.configureTestingModule({
      imports: [GoogleDriveConnectComponent],
      providers: [{ provide: DriveService, useValue: driveServiceMock }],
      schemas: [NO_ERRORS_SCHEMA],
    }).compileComponents();

    fixture = TestBed.createComponent(GoogleDriveConnectComponent);
    component = fixture.componentInstance;
  });

  describe("ngOnInit", () => {
    it("sets status to connected when token is ok", () => {
      driveServiceMock.getToken.mockReturnValue(of({ status: "ok", accessToken: "abc" }));
      fixture.detectChanges();
      expect(component.status).toBe("connected");
    });

    it("sets status from response when token is not ok", () => {
      driveServiceMock.getToken.mockReturnValue(of({ status: "no_refresh_token" }));
      fixture.detectChanges();
      expect(component.status).toBe("no_refresh_token");
    });

    it("sets status to error when getToken fails", () => {
      driveServiceMock.getToken.mockReturnValue(throwError(() => new Error("network error")));
      fixture.detectChanges();
      expect(component.status).toBe("error");
    });

    it("sets status to connected when onConnected emits", () => {
      driveServiceMock.getToken.mockReturnValue(of({ status: "no_refresh_token" }));
      fixture.detectChanges();
      expect(component.status).toBe("no_refresh_token");

      connected$.next();
      expect(component.status).toBe("connected");
    });
  });

  describe("connect", () => {
    it("delegates to driveService.connect()", () => {
      component.connect();
      expect(driveServiceMock.connect).toHaveBeenCalledWith();
    });
  });

  describe("reconnect", () => {
    it("delegates to driveService.connect(true)", () => {
      component.reconnect();
      expect(driveServiceMock.connect).toHaveBeenCalledWith(true);
    });
  });

  describe("openFolderPicker", () => {
    it("sets selectedFolder when picker resolves", () => {
      const folder = { id: "folder-1", name: "My Exports" };
      driveServiceMock.openFolderPicker.mockReturnValue(of(folder));

      component.openFolderPicker();

      expect(component.selectedFolder).toEqual(folder);
    });

    it("does not set selectedFolder when picker is cancelled (completes without value)", () => {
      driveServiceMock.openFolderPicker.mockReturnValue(of());

      component.openFolderPicker();

      expect(component.selectedFolder).toBeNull();
    });
  });
});
