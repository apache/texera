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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { ActivatedRoute } from "@angular/router";
import { MarkdownService } from "ngx-markdown";
import { of, throwError } from "rxjs";

import { ModelDetailComponent } from "./model-detail.component";
import { ModelService } from "../../../../service/user/model/model.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { StubUserService } from "../../../../../common/service/user/stub-user.service";
import { DashboardModel } from "../../../../type/dashboard-model.interface";
import { ModelVersion } from "../../../../../common/type/model";
import { DatasetFileNode } from "../../../../../common/type/datasetVersionFileTree";
import { commonTestImports, commonTestProviders } from "../../../../../common/testing/test-utils";

function makeDashboardModel(overrides: Partial<DashboardModel["model"]> = {}, top: Partial<DashboardModel> = {}) {
  return {
    isOwner: true,
    ownerEmail: "owner@example.com",
    accessPrivilege: "WRITE",
    size: 100,
    model: {
      mid: 5,
      ownerUid: 1,
      name: "resnet",
      repositoryName: "model-5",
      isPublic: false,
      isDownloadable: true,
      description: "a model",
      creationTime: 1000,
      coverImage: undefined,
      framework: "pytorch",
      format: "safetensors",
      ...overrides,
    },
    ...top,
  } as DashboardModel;
}

function makeVersion(overrides: Partial<ModelVersion> = {}): ModelVersion {
  return {
    mvid: 10,
    mid: 5,
    creatorUid: 1,
    name: "v1",
    versionHash: "commit-abc",
    creationTime: 2000,
    fileNodes: undefined,
    ...overrides,
  };
}

// The backend roots model trees at the models prefix, so node paths resolve against models.
const VERSION_ROOT = "/models/owner@example.com/resnet/v1";

function fileNode(name: string, parentDir = VERSION_ROOT): DatasetFileNode {
  return { name, type: "file", parentDir, size: 8 };
}

function dirNode(name: string, children: DatasetFileNode[]): DatasetFileNode {
  return { name, type: "directory", parentDir: VERSION_ROOT, children };
}

describe("ModelDetailComponent", () => {
  let component: ModelDetailComponent;
  let fixture: ComponentFixture<ModelDetailComponent>;
  let modelService: {
    getModel: ReturnType<typeof vi.fn>;
    retrieveModelVersionList: ReturnType<typeof vi.fn>;
    retrieveModelVersionFileTree: ReturnType<typeof vi.fn>;
    updateModelPublicity: ReturnType<typeof vi.fn>;
    updateModelDownloadable: ReturnType<typeof vi.fn>;
    updateModelDescription: ReturnType<typeof vi.fn>;
  };
  let downloadService: {
    downloadModelVersion: ReturnType<typeof vi.fn>;
    downloadModelSingleFile: ReturnType<typeof vi.fn>;
  };
  let notificationService: { success: ReturnType<typeof vi.fn>; error: ReturnType<typeof vi.fn> };

  beforeEach(async () => {
    modelService = {
      getModel: vi.fn().mockReturnValue(of(makeDashboardModel())),
      retrieveModelVersionList: vi.fn().mockReturnValue(of([makeVersion()])),
      retrieveModelVersionFileTree: vi.fn().mockReturnValue(of({ fileNodes: [fileNode("model.pt")], size: 2048 })),
      updateModelPublicity: vi.fn().mockReturnValue(of({})),
      updateModelDownloadable: vi.fn().mockReturnValue(of({})),
      updateModelDescription: vi.fn().mockReturnValue(of({})),
    };
    downloadService = {
      downloadModelVersion: vi.fn().mockReturnValue(of(new Blob())),
      downloadModelSingleFile: vi.fn().mockReturnValue(of(new Blob())),
    };
    notificationService = { success: vi.fn(), error: vi.fn() };

    TestBed.configureTestingModule({
      imports: [ModelDetailComponent, ...commonTestImports],
      providers: [
        { provide: ModelService, useValue: modelService },
        { provide: DownloadService, useValue: downloadService },
        { provide: NotificationService, useValue: notificationService },
        { provide: UserService, useClass: StubUserService },
        { provide: ActivatedRoute, useValue: { params: of({ mid: "5" }), data: of({}) } },
        { provide: MarkdownService, useValue: { parse: vi.fn(() => "") } },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ModelDetailComponent);
    component = fixture.componentInstance;
  });

  describe("ngOnInit", () => {
    it("coerces the route param to a number and loads the model plus its versions", () => {
      fixture.detectChanges();

      // params["mid"] is a string; leaving it uncoerced would make mid === "5".
      expect(component.mid).toBe(5);
      expect(modelService.getModel).toHaveBeenCalledWith(5, true);
      expect(modelService.retrieveModelVersionList).toHaveBeenCalledWith(5);
    });

    it("populates the header fields, including the model-only framework and format", () => {
      fixture.detectChanges();

      expect(component.modelName).toBe("resnet");
      expect(component.modelDescription).toBe("a model");
      expect(component.modelFramework).toBe("pytorch");
      expect(component.modelFormat).toBe("safetensors");
      expect(component.userModelAccessLevel).toBe("WRITE");
      expect(component.ownerEmail).toBe("owner@example.com");
      expect(component.modelCreationTime).not.toBe("");
    });
  });

  describe("version selection", () => {
    it("selects the newest version and loads its file tree and size", () => {
      fixture.detectChanges();

      expect(component.selectedVersion?.name).toBe("v1");
      expect(modelService.retrieveModelVersionFileTree).toHaveBeenCalledWith(5, 10);
      expect(component.currentModelVersionSize).toBe(2048);
      expect(component.selectedVersionCreationTime).not.toBe("");
    });

    it("previews the first leaf, addressed by its full logical path", () => {
      modelService.retrieveModelVersionFileTree.mockReturnValue(
        of({ fileNodes: [dirNode("weights", [fileNode("model.pt", `${VERSION_ROOT}/weights`)])], size: 10 })
      );

      fixture.detectChanges();

      expect(component.currentDisplayedFileName).toBe(`${VERSION_ROOT}/weights/model.pt`);
      expect(component.currentFileSize).toBe(8);
    });

    it("clears the preview when the version has no files", () => {
      modelService.retrieveModelVersionFileTree.mockReturnValue(of({ fileNodes: [], size: 0 }));

      fixture.detectChanges();

      expect(component.currentDisplayedFileName).toBe("");
      expect(component.currentFileSize).toBeUndefined();
    });

    it("leaves no version selected when the model has none", () => {
      modelService.retrieveModelVersionList.mockReturnValue(of([]));

      fixture.detectChanges();

      expect(component.selectedVersion).toBeUndefined();
      expect(modelService.retrieveModelVersionFileTree).not.toHaveBeenCalled();
    });
  });

  describe("access and download gating", () => {
    it("userHasWriteAccess is true only for WRITE", () => {
      component.userModelAccessLevel = "WRITE";
      expect(component.userHasWriteAccess()).toBe(true);
      component.userModelAccessLevel = "READ";
      expect(component.userHasWriteAccess()).toBe(false);
    });

    it("allows an owner to download regardless of the downloadable flag", () => {
      component.isOwner = true;
      component.modelIsDownloadable = false;
      expect(component.isDownloadAllowed()).toBe(true);
    });

    it("allows a non-owner on a public downloadable model", () => {
      component.isOwner = false;
      component.modelIsDownloadable = true;
      component.modelIsPublic = true;
      component.userModelAccessLevel = "NONE";
      expect(component.isDownloadAllowed()).toBe(true);
    });

    it("blocks a non-owner without access on a private model", () => {
      component.isOwner = false;
      component.modelIsDownloadable = true;
      component.modelIsPublic = false;
      component.userModelAccessLevel = "NONE";
      expect(component.isDownloadAllowed()).toBe(false);
    });

    it("blocks a non-owner when downloads are restricted", () => {
      component.isOwner = false;
      component.modelIsDownloadable = false;
      component.modelIsPublic = true;
      expect(component.isDownloadAllowed()).toBe(false);
    });
  });

  describe("downloads", () => {
    it("downloads the selected version as a zip", () => {
      fixture.detectChanges();

      component.onClickDownloadVersionAsZip();

      expect(downloadService.downloadModelVersion).toHaveBeenCalledWith(5, 10, "resnet", "v1");
    });

    it("downloads the current file by its logical path", () => {
      fixture.detectChanges();

      component.onClickDownloadCurrentFile();

      expect(downloadService.downloadModelSingleFile).toHaveBeenCalledWith(`${VERSION_ROOT}/model.pt`, true);
    });

    it("uses the public endpoint for a non-owner on a public model", () => {
      fixture.detectChanges();
      component.isOwner = false;
      component.modelIsPublic = true;

      component.onClickDownloadCurrentFile();

      expect(downloadService.downloadModelSingleFile).toHaveBeenCalledWith(`${VERSION_ROOT}/model.pt`, false);
    });
  });

  describe("publicity and downloadable toggles", () => {
    it("updates publicity and reports the new state", () => {
      fixture.detectChanges();

      component.onPublicStatusChange(true);

      expect(modelService.updateModelPublicity).toHaveBeenCalledWith(5);
      expect(component.modelIsPublic).toBe(true);
      expect(notificationService.success).toHaveBeenCalledWith("Model resnet is now public");
    });

    it("leaves the flag untouched when the publicity update fails", () => {
      fixture.detectChanges();
      modelService.updateModelPublicity.mockReturnValue(throwError(() => new Error("nope")));

      component.onPublicStatusChange(true);

      expect(component.modelIsPublic).toBe(false);
      expect(notificationService.error).toHaveBeenCalledWith("Fail to change the model publicity");
    });

    it("updates the downloadable flag and reports the new state", () => {
      fixture.detectChanges();

      component.onDownloadableStatusChange(false);

      expect(modelService.updateModelDownloadable).toHaveBeenCalledWith(5);
      expect(component.modelIsDownloadable).toBe(false);
      expect(notificationService.success).toHaveBeenCalledWith("Model downloads are now not allowed");
    });
  });

  describe("description", () => {
    it("persists a changed description", () => {
      fixture.detectChanges();

      component.onModelDescriptionChange("updated");

      expect(modelService.updateModelDescription).toHaveBeenCalledWith(5, "updated");
      expect(component.modelDescription).toBe("updated");
    });

    it("does not call the backend when the description is unchanged", () => {
      fixture.detectChanges();

      component.onModelDescriptionChange("a model");

      expect(modelService.updateModelDescription).not.toHaveBeenCalled();
    });

    it("reverts to the previous description when the update fails", () => {
      fixture.detectChanges();
      modelService.updateModelDescription.mockReturnValue(throwError(() => new Error("nope")));

      component.onModelDescriptionChange("updated");

      expect(component.modelDescription).toBe("a model");
      expect(notificationService.error).toHaveBeenCalledWith("Failed to update model description");
    });
  });

  describe("view controls", () => {
    it("toggles the maximized and collapsed flags", () => {
      component.onClickScaleTheView();
      expect(component.isMaximized).toBe(true);
      component.onClickHideRightBar();
      expect(component.isRightBarCollapsed).toBe(true);
    });

    it("selecting a tree node loads its full logical path", () => {
      component.onVersionFileTreeNodeSelected(fileNode("config.json"));

      expect(component.currentDisplayedFileName).toBe(`${VERSION_ROOT}/config.json`);
    });
  });

  describe("copyCurrentFilePath", () => {
    it("copies the displayed path and reports success", async () => {
      const writeText = vi.fn().mockResolvedValue(undefined);
      Object.assign(navigator, { clipboard: { writeText } });
      component.currentDisplayedFileName = "weights/model.pt";

      await component.copyCurrentFilePath();

      expect(writeText).toHaveBeenCalledWith("weights/model.pt");
      expect(notificationService.success).toHaveBeenCalledWith("File path copied to clipboard");
    });

    it("does nothing when no file is displayed", async () => {
      const writeText = vi.fn();
      Object.assign(navigator, { clipboard: { writeText } });
      component.currentDisplayedFileName = "";

      await component.copyCurrentFilePath();

      expect(writeText).not.toHaveBeenCalled();
    });

    it("reports an error when the clipboard write is rejected", async () => {
      Object.assign(navigator, { clipboard: { writeText: vi.fn().mockRejectedValue(new Error("denied")) } });
      component.currentDisplayedFileName = "weights/model.pt";

      await component.copyCurrentFilePath();

      expect(notificationService.error).toHaveBeenCalledWith("Failed to copy file path");
    });
  });
});
