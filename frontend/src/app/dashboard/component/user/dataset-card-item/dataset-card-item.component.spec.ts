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
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { RouterTestingModule } from "@angular/router/testing";
import { of, throwError } from "rxjs";
import type { Mocked } from "vitest";

import { DatasetCardItemComponent } from "./dataset-card-item.component";
import { DashboardEntry } from "src/app/dashboard/type/dashboard-entry";
import { DatasetService } from "../../../service/user/dataset/dataset.service";
import { HubService } from "../../../../hub/service/hub.service";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { HUB_DATASET_RESULT_DETAIL, USER_DATASET } from "../../../../app-routing.constant";
import { commonTestProviders } from "../../../../common/testing/test-utils";

function makeDatasetEntry(overrides: Partial<any> = {}): DashboardEntry {
  return {
    type: "dataset",
    id: 42,
    accessibleUserIds: [1, 2],
    coverImageUrl: undefined,
    likeCount: 5,
    isLiked: false,
    ...overrides,
  } as unknown as DashboardEntry;
}

describe("DatasetCardItemComponent", () => {
  let component: DatasetCardItemComponent;
  let fixture: ComponentFixture<DatasetCardItemComponent>;
  let hubService: Mocked<HubService>;

  beforeEach(async () => {
    const hubServiceSpy = {
      toggleLike: vi.fn().mockReturnValue(of({ liked: true, likeCount: 7 })),
    };

    await TestBed.configureTestingModule({
      imports: [DatasetCardItemComponent, HttpClientTestingModule, BrowserAnimationsModule, RouterTestingModule],
      providers: [
        {
          provide: DatasetService,
          useValue: {
            getDatasetCoverUrl: vi.fn().mockReturnValue(of({ url: "https://s3.example/presigned" })),
          },
        },
        { provide: HubService, useValue: hubServiceSpy },
        { provide: UserService, useClass: StubUserService },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(DatasetCardItemComponent);
    component = fixture.componentInstance;
    hubService = TestBed.inject(HubService) as unknown as Mocked<HubService>;
  });

  describe("entryLink", () => {
    it("routes to the private dataset page when the current user has access", () => {
      component.currentUid = 1;
      component.entry = makeDatasetEntry({ id: 99, accessibleUserIds: [1, 2] });
      component.ngOnChanges({ entry: { currentValue: component.entry } } as any);
      expect(component.entryLink).toEqual([USER_DATASET, "99"]);
    });

    it("routes to the hub detail page when the current user has no access", () => {
      component.currentUid = 5;
      component.entry = makeDatasetEntry({ id: 99, accessibleUserIds: [1, 2] });
      component.ngOnChanges({ entry: { currentValue: component.entry } } as any);
      expect(component.entryLink).toEqual([HUB_DATASET_RESULT_DETAIL, "99"]);
    });
  });

  describe("coverImageSrc", () => {
    it("falls back to the default cover when coverImageUrl is missing", () => {
      const datasetService = TestBed.inject(DatasetService) as unknown as Mocked<DatasetService>;
      component.entry = makeDatasetEntry({ coverImageUrl: undefined });
      component.ngOnChanges({ entry: { currentValue: component.entry } } as any);
      expect(component.coverImageSrc).toBe(component.defaultCover);
      expect(datasetService.getDatasetCoverUrl).not.toHaveBeenCalled();
    });

    it("swaps in the presigned URL once the backend resolves it", () => {
      const datasetService = TestBed.inject(DatasetService) as unknown as Mocked<DatasetService>;
      component.entry = makeDatasetEntry({ id: 7, coverImageUrl: "v1/img.png" });
      component.ngOnChanges({ entry: { currentValue: component.entry } } as any);
      expect(datasetService.getDatasetCoverUrl).toHaveBeenCalledWith(7);
      expect(component.coverImageSrc).toBe("https://s3.example/presigned");
    });

    it("falls back to the default cover when the backend returns a null url", () => {
      const datasetService = TestBed.inject(DatasetService) as unknown as Mocked<DatasetService>;
      datasetService.getDatasetCoverUrl.mockReturnValueOnce(of({ url: null }));
      component.entry = makeDatasetEntry({ id: 9, coverImageUrl: "v1/img.png" });
      component.ngOnChanges({ entry: { currentValue: component.entry } } as any);
      expect(component.coverImageSrc).toBe(component.defaultCover);
    });

    it("falls back to the default cover when the backend errors", () => {
      const datasetService = TestBed.inject(DatasetService) as unknown as Mocked<DatasetService>;
      datasetService.getDatasetCoverUrl.mockReturnValueOnce(throwError(() => new Error("403")));
      component.entry = makeDatasetEntry({ id: 11, coverImageUrl: "v1/img.png" });
      component.ngOnChanges({ entry: { currentValue: component.entry } } as any);
      expect(component.coverImageSrc).toBe(component.defaultCover);
    });
  });

  describe("toggleLike", () => {
    beforeEach(() => {
      component.currentUid = 1;
      component.entry = makeDatasetEntry();
      component.ngOnChanges({ entry: { currentValue: component.entry } } as any);
    });

    it("does nothing when the user is not signed in", () => {
      component.currentUid = undefined;
      component.toggleLike();
      expect(hubService.toggleLike).not.toHaveBeenCalled();
    });

    it("toggles to liked and reconciles state from the server", () => {
      component.isLiked = false;
      component.toggleLike();
      expect(hubService.toggleLike).toHaveBeenCalledWith(42, "dataset", false);
      expect(component.isLiked).toBe(true);
      expect(component.likeCount).toBe(7);
    });

    it("toggles to unliked and reconciles state from the server", () => {
      hubService.toggleLike.mockReturnValueOnce(of({ liked: false, likeCount: 6 }));
      component.isLiked = true;
      component.toggleLike();
      expect(hubService.toggleLike).toHaveBeenCalledWith(42, "dataset", true);
      expect(component.isLiked).toBe(false);
      expect(component.likeCount).toBe(6);
    });
  });
});
