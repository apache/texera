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

import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { ResultTableFrameComponent } from "./result-table-frame.component";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalModule } from "ng-zorro-antd/modal";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { AdminSettingsService } from "../../../../dashboard/service/admin/settings/admin-settings.service";
import { Observable, of, throwError } from "rxjs";

describe("ResultTableFrameComponent", () => {
  let component: ResultTableFrameComponent;
  let fixture: ComponentFixture<ResultTableFrameComponent>;

  const GUI_CONFIG_LIMIT = 15;

  // Build the test bed with a configurable AdminSettingsService stub so individual
  // tests can vary how the result_table_columns_per_batch lookup behaves.
  // The real service maps missing rows to null, so the stub mirrors that surface.
  const setupWith = (getSetting: (key: string) => Observable<string | null>) => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, NzModalModule],
      declarations: [ResultTableFrameComponent],
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        {
          provide: GuiConfigService,
          useValue: {
            env: {
              limitColumns: GUI_CONFIG_LIMIT,
            },
          },
        },
        {
          provide: AdminSettingsService,
          useValue: { getSetting },
        },
        ...commonTestProviders,
      ],
    }).compileComponents();
    fixture = TestBed.createComponent(ResultTableFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  };

  beforeEach(waitForAsync(() => {
    setupWith(() => of("15"));
  }));

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("currentResult should not be modified if setupResultTable is called with empty (zero-length) execution result  ", () => {
    component.currentResult = [{ test: "property" }];
    (component as any).setupResultTable([]);

    expect(component.currentResult).toEqual([{ test: "property" }]);
  });

  it("should set columnLimit from config", () => {
    expect(component.columnLimit).toEqual(15);
  });

  describe("Result Panel — admin setting consumption", () => {
    it("uses the admin-settings value when it is a positive integer, overriding gui-config", waitForAsync(() => {
      setupWith(() => of("42"));
      expect(component.columnLimit).toBe(42);
    }));

    it("falls back to gui-config limitColumns when admin-settings returns a non-positive value", waitForAsync(() => {
      setupWith(() => of("0"));
      expect(component.columnLimit).toBe(GUI_CONFIG_LIMIT);
    }));

    it("falls back to gui-config limitColumns when admin-settings returns an unparseable value", waitForAsync(() => {
      setupWith(() => of("not-a-number"));
      expect(component.columnLimit).toBe(GUI_CONFIG_LIMIT);
    }));

    it("falls back to gui-config limitColumns when admin-settings returns null", waitForAsync(() => {
      setupWith(() => of(null));
      expect(component.columnLimit).toBe(GUI_CONFIG_LIMIT);
    }));

    it("falls back to gui-config limitColumns when admin-settings errors", waitForAsync(() => {
      setupWith(() => throwError(() => new Error("network down")));
      expect(component.columnLimit).toBe(GUI_CONFIG_LIMIT);
    }));
  });
});
