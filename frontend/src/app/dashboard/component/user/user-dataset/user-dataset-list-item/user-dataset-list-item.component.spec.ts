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

import { Component, EventEmitter, ViewChild } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { provideRouter } from "@angular/router";
import { NzListComponent } from "ng-zorro-antd/list";
import { NzModalService } from "ng-zorro-antd/modal";
import { of, throwError } from "rxjs";
import type { Mocked } from "vitest";
import { UserDatasetListItemComponent } from "./user-dataset-list-item.component";
import { DatasetService } from "../../../../service/user/dataset/dataset.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { ShareAccessComponent } from "../../share-access/share-access.component";
import { DashboardDataset } from "../../../../type/dashboard-dataset.interface";
import { commonTestProviders } from "../../../../../common/testing/test-utils";

// UserDatasetListItemComponent is rooted at <nz-list-item>; instantiating it
// outside an <nz-list> host throws "No provider found for NzListComponent".
@Component({
  standalone: true,
  imports: [NzListComponent, UserDatasetListItemComponent],
  template: `
    <nz-list>
      <texera-user-dataset-list-item
        [entry]="entry"
        [editable]="editable"></texera-user-dataset-list-item>
    </nz-list>
  `,
})
class TestHostComponent {
  entry!: DashboardDataset;
  editable = true;
  @ViewChild(UserDatasetListItemComponent, { static: true }) inner!: UserDatasetListItemComponent;
}

function makeEntry(overrides: Partial<DashboardDataset> = {}): DashboardDataset {
  return {
    isOwner: true,
    ownerEmail: "owner@example.com",
    accessPrivilege: "WRITE",
    size: 0,
    dataset: {
      did: 1,
      ownerUid: 1,
      name: "dataset-1",
      isPublic: false,
      isDownloadable: true,
      storagePath: undefined,
      description: "original description",
      creationTime: 0,
      coverImage: undefined,
    },
    ...overrides,
  };
}

describe("UserDatasetListItemComponent", () => {
  let component: UserDatasetListItemComponent;
  let fixture: ComponentFixture<TestHostComponent>;
  let datasetService: Mocked<DatasetService>;
  let notificationService: Mocked<NotificationService>;
  let modalService: NzModalService;

  beforeEach(async () => {
    const datasetServiceSpy = {
      updateDatasetName: vi.fn(),
      updateDatasetDescription: vi.fn(),
    };
    const notificationServiceSpy = {
      error: vi.fn(),
      success: vi.fn(),
      info: vi.fn(),
      warning: vi.fn(),
      loading: vi.fn(),
      blank: vi.fn(),
    };

    await TestBed.configureTestingModule({
      imports: [TestHostComponent, HttpClientTestingModule],
      providers: [
        { provide: DatasetService, useValue: datasetServiceSpy },
        { provide: NotificationService, useValue: notificationServiceSpy },
        NzModalService,
        provideRouter([]),
        ...commonTestProviders,
      ],
    }).compileComponents();

    datasetService = TestBed.inject(DatasetService) as unknown as Mocked<DatasetService>;
    notificationService = TestBed.inject(NotificationService) as unknown as Mocked<NotificationService>;
    modalService = TestBed.inject(NzModalService);
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TestHostComponent);
    fixture.componentInstance.entry = makeEntry();
    fixture.componentInstance.editable = true;
    fixture.detectChanges();
    component = fixture.componentInstance.inner;
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("entry getter", () => {
    it("throws when accessed before being set", () => {
      // Build a bare component without going through the host (the host always
      // assigns entry on first change detection).
      const bare = new UserDatasetListItemComponent(
        {} as NzModalService,
        {} as DatasetService,
        {} as NotificationService
      );
      expect(() => bare.entry).toThrowError("entry property must be provided to UserDatasetListItemComponent.");
    });

    it("returns the value once set", () => {
      const entry = makeEntry({ accessPrivilege: "READ" });
      const bare = new UserDatasetListItemComponent(
        {} as NzModalService,
        {} as DatasetService,
        {} as NotificationService
      );
      bare.entry = entry;
      expect(bare.entry).toBe(entry);
    });
  });

  describe("dataset getter", () => {
    it("returns entry.dataset when present", () => {
      expect(component.dataset).toBe(component.entry.dataset);
    });

    it("throws when entry.dataset is missing", () => {
      const bare = new UserDatasetListItemComponent(
        {} as NzModalService,
        {} as DatasetService,
        {} as NotificationService
      );
      bare.entry = { ...makeEntry(), dataset: undefined as unknown as DashboardDataset["dataset"] };
      expect(() => bare.dataset).toThrowError(
        "Incorrect type of DashboardEntry provided to UserDatasetListItemComponent. Entry must be dataset."
      );
    });
  });

  describe("confirmUpdateDatasetCustomName", () => {
    it("is a no-op when the name has not changed", () => {
      const current = component.entry.dataset.name;
      component.confirmUpdateDatasetCustomName(current);
      expect(datasetService.updateDatasetName).not.toHaveBeenCalled();
    });

    it("updates the dataset name on success and clears editingName", () => {
      const newName = "renamed-dataset";
      component.editingName = true;
      datasetService.updateDatasetName.mockReturnValue(of({} as Response));

      component.confirmUpdateDatasetCustomName(newName);

      expect(datasetService.updateDatasetName).toHaveBeenCalledExactlyOnceWith(1, newName);
      expect(component.entry.dataset.name).toBe(newName);
      expect(component.editingName).toBe(false);
    });

    it("notifies the user on error and still clears editingName", () => {
      const originalName = component.entry.dataset.name;
      component.editingName = true;
      datasetService.updateDatasetName.mockReturnValue(throwError(() => new Error("boom")));

      component.confirmUpdateDatasetCustomName("renamed-dataset");

      expect(notificationService.error).toHaveBeenCalledExactlyOnceWith("boom");
      expect(component.entry.dataset.name).toBe(originalName);
      expect(component.editingName).toBe(false);
    });

    ["", "a/b", "has space", "名前"].forEach(invalidName => {
      it(`rejects the invalid name '${invalidName}' without calling the service`, () => {
        const originalName = component.entry.dataset.name;
        component.editingName = true;

        component.confirmUpdateDatasetCustomName(invalidName);

        expect(datasetService.updateDatasetName).not.toHaveBeenCalled();
        expect(notificationService.error).toHaveBeenCalledExactlyOnceWith(
          "Invalid dataset name: only letters, numbers, underscores, and hyphens are allowed (max 128 characters)"
        );
        expect(component.entry.dataset.name).toBe(originalName);
        expect(component.editingName).toBe(false);
      });
    });

    it("surfaces the backend error message when the rename is rejected", () => {
      const originalName = component.entry.dataset.name;
      component.editingName = true;
      datasetService.updateDatasetName.mockReturnValue(
        throwError(() => ({ error: { message: "Dataset with the same name already exists" } }))
      );

      component.confirmUpdateDatasetCustomName("renamed-dataset");

      expect(notificationService.error).toHaveBeenCalledExactlyOnceWith("Dataset with the same name already exists");
      expect(component.entry.dataset.name).toBe(originalName);
      expect(component.editingName).toBe(false);
    });
  });

  describe("confirmUpdateDatasetCustomDescription", () => {
    it("is a no-op when the description has not changed", () => {
      const current = component.entry.dataset.description;
      component.confirmUpdateDatasetCustomDescription(current);
      expect(datasetService.updateDatasetDescription).not.toHaveBeenCalled();
    });

    it("updates the dataset description on success and clears editingDescription", () => {
      const newDescription = "updated description";
      component.editingDescription = true;
      datasetService.updateDatasetDescription.mockReturnValue(of({} as Response));

      component.confirmUpdateDatasetCustomDescription(newDescription);

      expect(datasetService.updateDatasetDescription).toHaveBeenCalledExactlyOnceWith(1, newDescription);
      expect(component.entry.dataset.description).toBe(newDescription);
      expect(component.editingDescription).toBe(false);
    });

    it("notifies the user on error and still clears editingDescription", () => {
      const originalDescription = component.entry.dataset.description;
      component.editingDescription = true;
      datasetService.updateDatasetDescription.mockReturnValue(throwError(() => new Error("boom")));

      component.confirmUpdateDatasetCustomDescription("updated description");

      expect(notificationService.error).toHaveBeenCalledExactlyOnceWith("Update dataset description failed");
      expect(component.entry.dataset.description).toBe(originalDescription);
      expect(component.editingDescription).toBe(false);
    });
  });

  describe("onClickOpenShareAccess", () => {
    it("opens the share-access modal with the expected nzData and options", () => {
      const createSpy = vi.spyOn(modalService, "create").mockReturnValue({} as any);

      component.onClickOpenShareAccess();

      expect(createSpy).toHaveBeenCalledExactlyOnceWith({
        nzContent: ShareAccessComponent,
        nzData: {
          writeAccess: true,
          type: "dataset",
          id: 1,
        },
        nzFooter: null,
        nzTitle: "Share this dataset with others",
        nzCentered: true,
      });
    });

    it("sets writeAccess to false when accessPrivilege is not WRITE", () => {
      fixture.componentInstance.entry = makeEntry({ accessPrivilege: "READ" });
      fixture.detectChanges();
      const inner = fixture.componentInstance.inner;
      const createSpy = vi.spyOn(modalService, "create").mockReturnValue({} as any);

      inner.onClickOpenShareAccess();

      expect(createSpy).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          nzData: expect.objectContaining({ writeAccess: false }),
        })
      );
    });
  });

  describe("inputs and outputs", () => {
    it("defaults editable to false", () => {
      const bare = new UserDatasetListItemComponent(
        {} as NzModalService,
        {} as DatasetService,
        {} as NotificationService
      );
      expect(bare.editable).toBe(false);
    });

    it("exposes deleted, duplicated, and refresh as EventEmitters", () => {
      expect(component.deleted).toBeInstanceOf(EventEmitter);
      expect(component.duplicated).toBeInstanceOf(EventEmitter);
      expect(component.refresh).toBeInstanceOf(EventEmitter);
    });
  });
});
