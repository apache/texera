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
import { NzCardModule } from "ng-zorro-antd/card";
import { NzMessageService } from "ng-zorro-antd/message";
import { of, throwError } from "rxjs";

import { AdminSettingsComponent } from "./admin-settings.component";
import { AdminSettingsService } from "../../../service/admin/settings/admin-settings.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";

describe("AdminSettingsComponent", () => {
  let component: AdminSettingsComponent;
  let fixture: ComponentFixture<AdminSettingsComponent>;
  let adminSettingsServiceSpy: jasmine.SpyObj<AdminSettingsService>;
  let notificationServiceSpy: jasmine.SpyObj<NotificationService>;
  let messageServiceSpy: jasmine.SpyObj<NzMessageService>;

  // Returns the stored value for known result-panel keys, falling back to
  // the value the GET endpoint would emit (null) when no row is present.
  const stubGetSetting = (overrides: Record<string, string | null> = {}) => {
    const defaults: Record<string, string | null> = {
      csv_parser_max_columns: "512",
      result_table_columns_per_batch: "15",
    };
    return (key: string) => of(overrides[key] ?? defaults[key] ?? null);
  };

  beforeEach(async () => {
    adminSettingsServiceSpy = jasmine.createSpyObj<AdminSettingsService>("AdminSettingsService", [
      "getSetting",
      "updateSetting",
      "resetSetting",
    ]);
    adminSettingsServiceSpy.getSetting.and.callFake(stubGetSetting());
    adminSettingsServiceSpy.updateSetting.and.returnValue(of(undefined as void));
    adminSettingsServiceSpy.resetSetting.and.returnValue(of(undefined as void));

    notificationServiceSpy = jasmine.createSpyObj<NotificationService>("NotificationService", [
      "success",
      "error",
      "info",
      "warning",
      "blank",
      "loading",
      "remove",
    ]);

    messageServiceSpy = jasmine.createSpyObj<NzMessageService>("NzMessageService", [
      "success",
      "error",
      "info",
      "warning",
    ]);

    await TestBed.configureTestingModule({
      declarations: [AdminSettingsComponent],
      imports: [HttpClientTestingModule, NzCardModule],
      providers: [
        { provide: AdminSettingsService, useValue: adminSettingsServiceSpy },
        { provide: NotificationService, useValue: notificationServiceSpy },
        { provide: NzMessageService, useValue: messageServiceSpy },
      ],
      schemas: [NO_ERRORS_SCHEMA],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminSettingsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("Result Panel settings — load", () => {
    it("populates csvMaxColumns and resultTableColumnsPerBatch from the service", () => {
      expect(component.csvMaxColumns).toBe(512);
      expect(component.resultTableColumnsPerBatch).toBe(15);
      expect(adminSettingsServiceSpy.getSetting).toHaveBeenCalledWith("csv_parser_max_columns");
      expect(adminSettingsServiceSpy.getSetting).toHaveBeenCalledWith("result_table_columns_per_batch");
    });

    it("falls back to defaults when the stored value is missing or unparseable", () => {
      adminSettingsServiceSpy.getSetting.and.callFake(
        stubGetSetting({ csv_parser_max_columns: null, result_table_columns_per_batch: "not-a-number" })
      );

      const reloaded = TestBed.createComponent(AdminSettingsComponent);
      reloaded.detectChanges();

      expect(reloaded.componentInstance.csvMaxColumns).toBe(512);
      expect(reloaded.componentInstance.resultTableColumnsPerBatch).toBe(15);
    });
  });

  describe("Result Panel settings — saveCsvSettings", () => {
    it("persists both values and emits a success notification on success", () => {
      component.csvMaxColumns = 1024;
      component.resultTableColumnsPerBatch = 25;

      component.saveCsvSettings();

      expect(adminSettingsServiceSpy.updateSetting).toHaveBeenCalledWith("csv_parser_max_columns", "1024");
      expect(adminSettingsServiceSpy.updateSetting).toHaveBeenCalledWith("result_table_columns_per_batch", "25");
      expect(notificationServiceSpy.success).toHaveBeenCalledTimes(1);
      expect(notificationServiceSpy.error).not.toHaveBeenCalled();
    });

    it("emits an error notification when the backend save fails", () => {
      adminSettingsServiceSpy.updateSetting.and.returnValue(throwError(() => new Error("boom")));
      component.csvMaxColumns = 1024;
      component.resultTableColumnsPerBatch = 25;

      component.saveCsvSettings();

      expect(notificationServiceSpy.error).toHaveBeenCalledTimes(1);
      expect(notificationServiceSpy.success).not.toHaveBeenCalled();
    });
  });

  describe("Result Panel settings — resetCsvSettings", () => {
    // resetCsvSettings schedules a window.location.reload() via setTimeout. Use the
    // jasmine clock so the timer never fires inside the test runner's iframe.
    beforeEach(() => jasmine.clock().install());
    afterEach(() => jasmine.clock().uninstall());

    it("issues reset requests for both keys and emits an info notification", () => {
      component.resetCsvSettings();

      expect(adminSettingsServiceSpy.resetSetting).toHaveBeenCalledWith("csv_parser_max_columns");
      expect(adminSettingsServiceSpy.resetSetting).toHaveBeenCalledWith("result_table_columns_per_batch");
      expect(notificationServiceSpy.info).toHaveBeenCalledTimes(1);
    });
  });
});
