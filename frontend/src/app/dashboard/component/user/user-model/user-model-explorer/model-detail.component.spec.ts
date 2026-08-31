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
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { ActivatedRoute } from "@angular/router";
import { of, throwError } from "rxjs";
import { MarkdownService } from "ngx-markdown";
import { NzModalService } from "ng-zorro-antd/modal";
import { NgModel } from "@angular/forms";
import { NzResizableDirective } from "ng-zorro-antd/resizable";
import { By } from "@angular/platform-browser";
import { commonTestImports, commonTestProviders } from "../../../../../common/testing/test-utils";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { StubUserService } from "../../../../../common/service/user/stub-user.service";
import { MODEL_FORMATS, MODEL_FRAMEWORKS, ModelService } from "../../../../service/user/model/model.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { AdminSettingsService } from "../../../../service/admin/settings/admin-settings.service";
import { MultipartUploadService } from "../../../../service/user/file-resource/multipart-upload.service";
import { StagedFileService } from "../../../../service/user/file-resource/staged-file.service";
import { MODEL_FILE_RESOURCE_ENDPOINT } from "../../../../service/user/file-resource/file-resource-endpoint";
import { VersionUploaderComponent } from "../../version-uploader/version-uploader.component";
import { MarkdownDescriptionComponent } from "../../markdown-description/markdown-description.component";
import { DatasetFileNode } from "../../../../../common/type/datasetVersionFileTree";
import { ModelVersion } from "../../../../../common/type/model";
import { Role, User } from "../../../../../common/type/user";
import { ModelDetailComponent } from "./model-detail.component";

const MID = 5;
const OWNER = "owner@texera.com";

const aVersion = (mvid: number, name: string, creationTime = 1700000000000): ModelVersion => ({
  mvid,
  mid: MID,
  creatorUid: 9,
  name,
  versionHash: `hash-${mvid}`,
  creationTime,
  fileNodes: undefined,
});

const aFile = (name: string, parentDir: string, size = 128): DatasetFileNode => ({
  name,
  type: "file",
  parentDir,
  size,
});

describe("ModelDetailComponent", () => {
  let fixture: ComponentFixture<ModelDetailComponent>;
  let component: ModelDetailComponent;
  let modelService: Record<string, ReturnType<typeof vi.fn>>;
  let downloadService: Record<string, ReturnType<typeof vi.fn>>;
  let notificationService: Record<string, ReturnType<typeof vi.fn>>;
  let multipartUploadService: Record<string, ReturnType<typeof vi.fn>>;
  let stagedFileService: Record<string, ReturnType<typeof vi.fn>>;
  let adminSettingsService: Record<string, ReturnType<typeof vi.fn>>;

  const dashboardModel = (overrides: Partial<Record<string, unknown>> = {}) => ({
    isOwner: true,
    ownerEmail: OWNER,
    accessPrivilege: "WRITE",
    size: 0,
    ...overrides,
    model: {
      mid: MID,
      ownerUid: 9,
      name: "resnet-50",
      repositoryName: "model-5",
      isPublic: false,
      isDownloadable: true,
      description: "a description",
      creationTime: 1699000000000,
      coverImage: undefined,
      framework: "pytorch",
      format: "torchscript",
      ...((overrides["model"] as object) ?? {}),
    },
  });

  /** Rebuilds the fixture so per-test service stubs are in place before ngOnInit runs. */
  const create = (): void => {
    fixture = TestBed.createComponent(ModelDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  };

  beforeEach(() => {
    TestBed.resetTestingModule();

    modelService = {
      getModel: vi.fn(() => of(dashboardModel())),
      retrieveModelVersionList: vi.fn(() => of([])),
      retrieveModelVersionFileTree: vi.fn(() => of({ fileNodes: [], size: 0 })),
      // The real file renderer is rendered here, and it fetches whatever file is on screen.
      retrieveModelVersionSingleFile: vi.fn(() => of(new Blob(["hi"], { type: "text/plain" }))),
      getModelCoverUrl: vi.fn(() => of({ url: "http://cover" })),
    };
    downloadService = {
      downloadModelSingleFile: vi.fn(() => of(new Blob())),
      downloadModelVersion: vi.fn(() => of(new Blob())),
    };
    notificationService = { success: vi.fn(), error: vi.fn(), info: vi.fn() };
    multipartUploadService = {
      multipartUpload: vi.fn(() => of({ filePath: "f", percentage: 100, status: "finished", totalTime: 1 })),
      listMultipartUploads: vi.fn(() => of([])),
      findExistingUploadFiles: vi.fn(() => of([])),
      finalizeMultipartUpload: vi.fn(() => of({})),
    };
    stagedFileService = {
      getDiff: vi.fn(() => of([])),
      resetFileDiff: vi.fn(() => of({})),
      deleteFile: vi.fn(() => of({})),
    };
    // Every model upload key is absent in this stub, so the component keeps its own defaults.
    adminSettingsService = { getPublicSetting: vi.fn(() => of("")) };

    TestBed.configureTestingModule({
      imports: [ModelDetailComponent, NoopAnimationsModule, ...commonTestImports],
      providers: [
        { provide: ActivatedRoute, useValue: { params: of({ mid: String(MID) }), data: of({}) } },
        { provide: ModelService, useValue: modelService },
        { provide: DownloadService, useValue: downloadService },
        { provide: NotificationService, useValue: notificationService },
        { provide: UserService, useClass: StubUserService },
        { provide: MarkdownService, useValue: { parse: vi.fn(() => "") } },
        { provide: NzModalService, useValue: {} },
        { provide: MultipartUploadService, useValue: multipartUploadService },
        { provide: StagedFileService, useValue: stagedFileService },
        { provide: AdminSettingsService, useValue: adminSettingsService },
        ...commonTestProviders,
      ],
    });
  });

  afterEach(() => {
    fixture?.destroy();
  });

  /** Applies state on top of what ngOnInit produced and renders it. */
  const render = (state: Partial<ModelDetailComponent> = {}): HTMLElement => {
    Object.assign(component, state);
    fixture.detectChanges();
    return fixture.nativeElement as HTMLElement;
  };

  // nz-tabs only instantiates the active tab, so a tab has to be opened before
  // anything inside it exists to assert on.
  const openTab = (title: string): HTMLElement => {
    const tab = Array.from((fixture.nativeElement as HTMLElement).querySelectorAll<HTMLElement>(".ant-tabs-tab")).find(
      el => (el.textContent ?? "").includes(title)
    );
    expect(tab, `expected a tab titled "${title}"`).toBeDefined();
    tab!.click();
    fixture.detectChanges();
    return fixture.nativeElement as HTMLElement;
  };

  const q = <E extends Element>(root: ParentNode, selector: string): E => {
    const el = root.querySelector(selector);
    expect(el, `expected to find "${selector}"`).not.toBeNull();
    return el as unknown as E;
  };

  // The sider coalesces resize events into one write per animation frame.
  const nextFrame = (): Promise<void> => new Promise<void>(resolve => requestAnimationFrame(() => resolve()));

  // ─── loading the model ──────────────────────────────────────────────────────

  it("reads the mid off the route as a number and loads the model", () => {
    create();

    expect(component.mid).toBe(MID);
    expect(modelService["getModel"]).toHaveBeenCalledWith(MID, true);
    // Every read here picks its endpoint off isLogin; routing a signed-in user's version
    // listing through the anonymous one returns nothing at all for a private model.
    expect(modelService["retrieveModelVersionList"]).toHaveBeenCalledWith(MID, true);
    expect(component.modelName).toBe("resnet-50");
    expect(component.modelDescription).toBe("a description");
    expect(component.modelFramework).toBe("pytorch");
    expect(component.modelFormat).toBe("torchscript");
    expect(component.ownerEmail).toBe(OWNER);
    expect(component.userModelAccessLevel).toBe("WRITE");
    expect(component.modelCreationTime).not.toBe("");
  });

  it("skips the cover fetch for a model that has none", () => {
    create();

    expect(modelService["getModelCoverUrl"]).not.toHaveBeenCalled();
    expect(component.coverImageUrl).toBeNull();
  });

  it("resolves a presigned cover url only when the model carries a cover", () => {
    modelService["getModel"] = vi.fn(() => of(dashboardModel({ model: { coverImage: "v1/cover.png" } })));
    create();

    expect(modelService["getModelCoverUrl"]).toHaveBeenCalledWith(MID);
    expect(component.coverImageUrl).toBe("http://cover");
  });

  it("falls back to no cover when the presign call fails", () => {
    modelService["getModel"] = vi.fn(() => of(dashboardModel({ model: { coverImage: "v1/cover.png" } })));
    modelService["getModelCoverUrl"] = vi.fn(() => throwError(() => new Error("boom")));
    create();

    expect(component.coverImageUrl).toBeNull();
  });

  // ─── versions and the file tree ─────────────────────────────────────────────

  it("selects the newest version and opens its first file", () => {
    const versions = [aVersion(2, "v2", 1700000000000), aVersion(1, "v1", 1600000000000)];
    modelService["retrieveModelVersionList"] = vi.fn(() => of(versions));
    modelService["retrieveModelVersionFileTree"] = vi.fn(() =>
      of({ fileNodes: [aFile("model.pt", `/model/${OWNER}/resnet-50/v2`, 512)], size: 512 })
    );
    create();

    expect(component.selectedVersion).toBe(versions[0]);
    expect(modelService["retrieveModelVersionFileTree"]).toHaveBeenCalledWith(MID, 2, true);
    expect(component.currentDisplayedFileName).toBe(`/model/${OWNER}/resnet-50/v2/model.pt`);
    expect(component.currentFileSize).toBe(512);
    expect(component.currentModelVersionSize).toBe(512);
    expect(component.latestVersionFileName).toBe(`/model/${OWNER}/resnet-50/v2/model.pt`);
    expect(component.latestVersionSize).toBe(512);
    expect(component.latestVersionCreationTime).not.toBe("");
  });

  it("descends into directories to find the file it opens first", () => {
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(1, "v1")]));
    modelService["retrieveModelVersionFileTree"] = vi.fn(() =>
      of({
        fileNodes: [
          {
            name: "weights",
            type: "directory",
            parentDir: `/model/${OWNER}/resnet-50/v1`,
            children: [aFile("model.pt", `/model/${OWNER}/resnet-50/v1/weights`)],
          } as DatasetFileNode,
        ],
        size: 128,
      })
    );
    create();

    expect(component.currentDisplayedFileName).toBe(`/model/${OWNER}/resnet-50/v1/weights/model.pt`);
  });

  it("clears the open file when the selected version holds none", () => {
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(1, "v1")]));
    create();

    expect(component.currentDisplayedFileName).toBe("");
    expect(component.currentFileSize).toBeUndefined();
    expect(component.latestVersionFileName).toBe("");
  });

  it("leaves nothing selected for a model with no versions", () => {
    create();

    expect(component.versions).toEqual([]);
    expect(component.selectedVersion).toBeUndefined();
    expect(modelService["retrieveModelVersionFileTree"]).not.toHaveBeenCalled();
  });

  it("keeps the latest-version facts when an older version is selected", () => {
    const versions = [aVersion(2, "v2"), aVersion(1, "v1")];
    modelService["retrieveModelVersionList"] = vi.fn(() => of(versions));
    modelService["retrieveModelVersionFileTree"] = vi.fn((_mid: number, mvid: number) =>
      of({
        fileNodes: [aFile(`v${mvid}.pt`, `/model/${OWNER}/resnet-50/v${mvid}`)],
        size: mvid * 100,
      })
    );
    create();
    const latestFileName = component.latestVersionFileName;

    component.onVersionSelected(versions[1]);

    expect(component.currentDisplayedFileName).toBe(`/model/${OWNER}/resnet-50/v1/v1.pt`);
    expect(component.currentModelVersionSize).toBe(100);
    // The Model Card still describes the newest version, not the one being browsed.
    expect(component.latestVersionFileName).toBe(latestFileName);
    expect(component.latestVersionSize).toBe(200);
  });

  it("survives the version select being cleared", () => {
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(1, "v1")]));
    create();

    expect(() => component.onVersionSelected(undefined)).not.toThrow();
    expect(component.selectedVersion).toBeUndefined();
  });

  it("reports a failure instead of rendering a blank page", () => {
    // /model/list degrades an unreadable repository size to 0, while /model/{mid} throws,
    // so a model that lists fine can still 500 here.
    modelService["getModel"] = vi.fn(() => throwError(() => new Error("lakefs is down")));
    modelService["retrieveModelVersionList"] = vi.fn(() => throwError(() => new Error("lakefs is down")));
    create();

    expect(notificationService["error"]).toHaveBeenCalledTimes(2);
  });

  it("refuses a route segment that is not a model id", () => {
    TestBed.overrideProvider(ActivatedRoute, { useValue: { params: of({ mid: "abc" }), data: of({}) } });
    create();

    expect(component.mid).toBeUndefined();
    expect(modelService["getModel"]).not.toHaveBeenCalled();
    expect(notificationService["error"]).toHaveBeenCalled();
  });

  it("opens the file a tree node points at", () => {
    create();

    component.onVersionFileTreeNodeSelected(aFile("notes.txt", "/model/a/b/v1", 64));

    expect(component.currentDisplayedFileName).toBe("/model/a/b/v1/notes.txt");
    expect(component.currentFileSize).toBe(64);
  });

  // ─── downloads ──────────────────────────────────────────────────────────────

  it("downloads the open file through the authenticated endpoint for an owner", () => {
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(1, "v1")]));
    modelService["retrieveModelVersionFileTree"] = vi.fn(() =>
      of({ fileNodes: [aFile("model.pt", `/model/${OWNER}/resnet-50/v1`)], size: 128 })
    );
    create();

    component.onClickDownloadCurrentFile();

    expect(downloadService["downloadModelSingleFile"]).toHaveBeenCalledWith(
      `/model/${OWNER}/resnet-50/v1/model.pt`,
      true
    );
  });

  it("downloads a public model's file through the anonymous endpoint for a non-owner", () => {
    modelService["getModel"] = vi.fn(() => of(dashboardModel({ isOwner: false, model: { isPublic: true } })));
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(1, "v1")]));
    modelService["retrieveModelVersionFileTree"] = vi.fn(() =>
      of({ fileNodes: [aFile("model.pt", `/model/${OWNER}/resnet-50/v1`)], size: 128 })
    );
    create();

    component.onClickDownloadCurrentFile();

    expect(downloadService["downloadModelSingleFile"]).toHaveBeenCalledWith(
      `/model/${OWNER}/resnet-50/v1/model.pt`,
      false
    );
  });

  it("downloads the selected version as a zip", () => {
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(3, "v3")]));
    create();

    component.onClickDownloadVersionAsZip();

    expect(downloadService["downloadModelVersion"]).toHaveBeenCalledWith(MID, 3, "resnet-50", "v3");
  });

  it("downloads nothing while no version is selected", () => {
    create();

    component.onClickDownloadCurrentFile();
    component.onClickDownloadVersionAsZip();

    expect(downloadService["downloadModelSingleFile"]).not.toHaveBeenCalled();
    expect(downloadService["downloadModelVersion"]).not.toHaveBeenCalled();
  });

  it("allows a download for an owner, and for a grantee only while downloads are permitted", () => {
    create();
    expect(component.isDownloadAllowed()).toBe(true);

    render({ isOwner: false, modelIsDownloadable: false, modelIsPublic: true });
    expect(component.isDownloadAllowed()).toBe(false);

    render({ isOwner: false, modelIsDownloadable: true, modelIsPublic: false, userModelAccessLevel: "NONE" });
    expect(component.isDownloadAllowed()).toBe(false);

    render({ isOwner: false, modelIsDownloadable: true, modelIsPublic: false, userModelAccessLevel: "READ" });
    expect(component.isDownloadAllowed()).toBe(true);
  });

  // ─── the file path clipboard ────────────────────────────────────────────────

  it("copies the open file's path and reports failure", async () => {
    create();
    const writeText = vi.fn(() => Promise.resolve());
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });

    render({ currentDisplayedFileName: "/model/a/b/v1/model.pt" });
    await component.copyCurrentFilePath();
    expect(writeText).toHaveBeenCalledWith("/model/a/b/v1/model.pt");
    expect(notificationService["success"]).toHaveBeenCalled();

    writeText.mockImplementationOnce(() => Promise.reject(new Error("denied")));
    await component.copyCurrentFilePath();
    expect(notificationService["error"]).toHaveBeenCalled();

    // Nothing open: no clipboard call at all.
    writeText.mockClear();
    render({ currentDisplayedFileName: "" });
    await component.copyCurrentFilePath();
    expect(writeText).not.toHaveBeenCalled();
  });

  // ─── template ───────────────────────────────────────────────────────────────

  it("renders the model's name, tags and stats", () => {
    create();
    const root = render();

    expect(q(root, "h2").textContent).toContain("resnet-50");
    expect(root.textContent).toContain("Private");
    expect(root.textContent).toContain("Downloadable");
    expect(root.textContent).toContain("pytorch");
    expect(root.textContent).toContain("torchscript");
  });

  it("shows view and like counters as placeholder zeros", () => {
    // The hub backend has no model entity type yet, so nothing populates these and
    // nothing may call the hub from this page.
    create();
    const tags = q(render(), ".status-tag-row").textContent ?? "";

    expect(tags.match(/\b0\b/g)?.length).toBe(2);
    expect(component.viewCount).toBe(0);
    expect(component.likeCount).toBe(0);
  });

  it("dashes out the latest-version facts for a model with no versions", () => {
    create();
    const stats = q(render(), ".data-card-stats").textContent ?? "";

    // "0 B" would assert a zero-byte version that does not exist; the card already
    // uses an em dash for an absent framework or format.
    expect(stats).not.toContain("0 B");
    expect(stats.match(/—/g)?.length).toBe(3);
  });

  it("shows the empty-version notice until a version exists", () => {
    create();
    const root = openTab("Versions & Files");

    expect(q(root, "nz-empty")).toBeTruthy();
    expect(root.querySelector("texera-user-dataset-file-renderer")).toBeNull();
  });

  it("hands the file renderer the model kind and the selected version", () => {
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(1, "v1")]));
    modelService["retrieveModelVersionFileTree"] = vi.fn(() =>
      of({ fileNodes: [aFile("notes.txt", `/model/${OWNER}/resnet-50/v1`)], size: 128 })
    );
    create();
    const root = openTab("Versions & Files");

    const renderer = q(root, "texera-user-dataset-file-renderer");
    expect(renderer).toBeTruthy();
    expect(modelService["retrieveModelVersionSingleFile"]).toHaveBeenCalledWith(
      `/model/${OWNER}/resnet-50/v1/notes.txt`,
      true
    );
  });

  it("renders a cover image only once one resolves", () => {
    create();
    expect(fixture.nativeElement.querySelector(".model-cover-image")).toBeNull();

    const root = render({ coverImageUrl: "http://cover" });
    expect(q<HTMLImageElement>(root, ".model-cover-image").src).toContain("http://cover");
  });

  // ─── uploading files and cutting a version ──────────────────────────────────
  //
  // The panel itself is covered by version-uploader.component.spec.ts; what matters here is that
  // the page hands it the model's own addressing, and what the page still owns around it.

  it("hands the version uploader the model endpoint and the model's identity", () => {
    create();
    const root = openTab("Versions & Files");

    const panel = q(root, "texera-version-uploader");
    const uploader = fixture.debugElement.query(By.directive(VersionUploaderComponent))
      .componentInstance as VersionUploaderComponent;

    expect(panel).toBeTruthy();
    expect(uploader.endpoint).toBe(MODEL_FILE_RESOURCE_ENDPOINT);
    expect(uploader.resourceId).toBe(MID);
    expect(uploader.resourceName).toBe("resnet-50");
    expect(uploader.ownerEmail).toBe(OWNER);
  });

  it("reads its upload settings from the model keys, not the dataset ones", () => {
    create();
    openTab("Versions & Files");
    const keys = new Set(adminSettingsService["getPublicSetting"].mock.calls.map(call => call[0]));

    // Three from the panel's tuning, plus the file-picker's per-file ceiling — the 2 GiB model
    // limit added by #8000, which models used to inherit from the dataset's 20 MiB.
    expect(keys).toEqual(
      new Set([
        MODEL_FILE_RESOURCE_ENDPOINT.chunkSizeSettingKey,
        MODEL_FILE_RESOURCE_ENDPOINT.maxConcurrentChunksSettingKey,
        MODEL_FILE_RESOURCE_ENDPOINT.maxConcurrentFilesSettingKey,
        MODEL_FILE_RESOURCE_ENDPOINT.maxFileSizeSettingKey,
      ])
    );
    expect([...keys].every(key => key.startsWith("model_"))).toBe(true);
  });

  it("shows the upload panel only to a user who can write", () => {
    create();
    expect(openTab("Versions & Files").querySelector("texera-version-uploader")).toBeTruthy();

    modelService["getModel"] = vi.fn(() => of(dashboardModel({ accessPrivilege: "READ", isOwner: false })));
    create();
    expect(openTab("Versions & Files").querySelector("texera-version-uploader")).toBeNull();
  });

  it("commits a version through ModelService", () => {
    modelService["createModelVersion"] = vi.fn(() => of(aVersion(1, "v1")));
    create();

    component.createModelVersion("v1").subscribe();

    expect(modelService["createModelVersion"]).toHaveBeenCalledWith(MID, "v1");
  });

  it("reloads the version list once the panel reports a new version", () => {
    create();
    expect(modelService["retrieveModelVersionList"]).toHaveBeenCalledTimes(1);

    component.onVersionCreated();

    expect(modelService["retrieveModelVersionList"]).toHaveBeenCalledTimes(2);
  });

  it("stages a deletion of an already-committed file", () => {
    create();
    openTab("Versions & Files");

    component.onPreviouslyUploadedFileDeleted(aFile("model.pt", `/model/${OWNER}/resnet-50/v1`));

    expect(stagedFileService["deleteFile"]).toHaveBeenCalledWith(MODEL_FILE_RESOURCE_ENDPOINT, MID, "model.pt");
    expect(notificationService["success"]).toHaveBeenCalled();
  });

  it("stages nothing and reports the failure when the deletion is rejected", () => {
    stagedFileService["deleteFile"] = vi.fn(() => throwError(() => new Error("boom")));
    create();
    openTab("Versions & Files");

    component.onPreviouslyUploadedFileDeleted(aFile("model.pt", `/model/${OWNER}/resnet-50/v1`));

    expect(notificationService["error"]).toHaveBeenCalledWith("Failed to delete the file");
  });

  it("deletes nothing without a model id", () => {
    create();
    component.mid = undefined;

    component.onPreviouslyUploadedFileDeleted(aFile("model.pt", `/model/${OWNER}/resnet-50/v1`));

    expect(stagedFileService["deleteFile"]).not.toHaveBeenCalled();
  });

  // ─── the Settings tab ───────────────────────────────────────────────────────

  it("hides the Settings tab from a reader", () => {
    modelService["getModel"] = vi.fn(() => of(dashboardModel({ accessPrivilege: "READ", isOwner: false })));
    create();

    const titles = Array.from(
      (fixture.nativeElement as HTMLElement).querySelectorAll<HTMLElement>(".ant-tabs-tab")
    ).map(el => el.textContent ?? "");
    expect(titles.some(title => title.includes("Settings"))).toBe(false);
  });

  it("renames the model from the Settings tab", () => {
    modelService["updateModelName"] = vi.fn(() => of({}));
    create();
    component.editedModelName = "resnet-101";

    component.onSaveModelName();

    expect(modelService["updateModelName"]).toHaveBeenCalledWith(MID, "resnet-101");
    expect(component.modelName).toBe("resnet-101");
  });

  it("refreshes the file tree after a rename, so paths carry the new name", () => {
    modelService["updateModelName"] = vi.fn(() => of({}));
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(1, "v1")]));
    modelService["retrieveModelVersionFileTree"] = vi.fn(() =>
      of({ fileNodes: [aFile("model.pt", `/model/${OWNER}/resnet-50/v1`)], size: 4 })
    );
    create();
    modelService["retrieveModelVersionFileTree"].mockClear();
    // The renamed model's tree comes back under the new path.
    modelService["retrieveModelVersionFileTree"] = vi.fn(() =>
      of({ fileNodes: [aFile("model.pt", `/model/${OWNER}/resnet-101/v1`)], size: 4 })
    );
    component.editedModelName = "resnet-101";

    component.onSaveModelName();

    // Preview and single-file download resolve a model by (owner, name), so a tree still
    // holding the old name would 404 on every file until the page was reloaded.
    expect(modelService["retrieveModelVersionFileTree"]).toHaveBeenCalledWith(MID, 1, true);
    expect(component.currentDisplayedFileName).toBe(`/model/${OWNER}/resnet-101/v1/model.pt`);
    // Once, not twice: the newest version is the one on screen, so the Model Card is filled
    // in from this very response instead of a second identical request.
    expect(modelService["retrieveModelVersionFileTree"]).toHaveBeenCalledTimes(1);
  });

  it("lists the newest version's objects once on load, not once per consumer", () => {
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(2, "v2"), aVersion(1, "v1")]));
    modelService["retrieveModelVersionFileTree"] = vi.fn(() =>
      of({ fileNodes: [aFile("model.pt", `/model/${OWNER}/resnet-50/v2`)], size: 4 })
    );
    create();

    // The file tree and the Model Card both describe v2 here, so one response serves both.
    expect(modelService["retrieveModelVersionFileTree"]).toHaveBeenCalledTimes(1);
    expect(component.latestVersionFileName).toBe(`/model/${OWNER}/resnet-50/v2/model.pt`);
    expect(component.latestVersionSize).toBe(4);
  });

  it("refreshes the Model Card too when an older version is on screen, without moving the picker", () => {
    const versions = [aVersion(2, "v2"), aVersion(1, "v1")];
    modelService["updateModelName"] = vi.fn(() => of({}));
    modelService["retrieveModelVersionList"] = vi.fn(() => of(versions));
    let name = "resnet-50";
    modelService["retrieveModelVersionFileTree"] = vi.fn((_mid: number, mvid: number) =>
      of({ fileNodes: [aFile(`v${mvid}.pt`, `/model/${OWNER}/${name}/v${mvid}`)], size: mvid * 100 })
    );
    create();
    component.onVersionSelected(versions[1]);
    expect(component.latestVersionFileName).toBe(`/model/${OWNER}/resnet-50/v2/v2.pt`);

    name = "resnet-101";
    component.editedModelName = name;
    component.onSaveModelName();

    // The Model Card describes the newest version, which is not the one being browsed — deriving
    // its facts from the selection would have left this path on the old name.
    expect(component.latestVersionFileName).toBe(`/model/${OWNER}/resnet-101/v2/v2.pt`);
    expect(component.currentDisplayedFileName).toBe(`/model/${OWNER}/resnet-101/v1/v1.pt`);
    // Renaming is not a reason to move the user off the version they opened.
    expect(component.selectedVersion).toBe(versions[1]);
  });

  it("reopens the file you were reading after a rename, not the version's first", () => {
    modelService["updateModelName"] = vi.fn(() => of({}));
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(1, "v1")]));
    let name = "resnet-50";
    const tree = () => ({
      fileNodes: [
        aFile("first.txt", `/model/${OWNER}/${name}/v1`),
        {
          name: "weights",
          type: "directory" as const,
          parentDir: `/model/${OWNER}/${name}/v1`,
          children: [aFile("model.pt", `/model/${OWNER}/${name}/v1/weights`)],
        },
      ],
      size: 8,
    });
    modelService["retrieveModelVersionFileTree"] = vi.fn(() => of(tree()));
    create();
    // Open a nested file that is not the one the tree opens by default.
    component.onVersionFileTreeNodeSelected(aFile("model.pt", `/model/${OWNER}/resnet-50/v1/weights`));
    expect(component.currentDisplayedFileName).toBe(`/model/${OWNER}/resnet-50/v1/weights/model.pt`);

    name = "resnet-101";
    component.editedModelName = name;
    component.onSaveModelName();

    // Same file, new path — renaming should not lose the reader's place.
    expect(component.currentDisplayedFileName).toBe(`/model/${OWNER}/resnet-101/v1/weights/model.pt`);
  });

  it("falls back to the first file when a rename outlives the file that was open", () => {
    modelService["updateModelName"] = vi.fn(() => of({}));
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(1, "v1")]));
    modelService["retrieveModelVersionFileTree"] = vi.fn(() =>
      of({ fileNodes: [aFile("only.txt", `/model/${OWNER}/resnet-101/v1`)], size: 4 })
    );
    create();
    component.editedModelName = "resnet-101";

    component.onSaveModelName();

    expect(component.currentDisplayedFileName).toBe(`/model/${OWNER}/resnet-101/v1/only.txt`);
  });

  it("skips the tree refresh for a model with no versions", () => {
    modelService["updateModelName"] = vi.fn(() => of({}));
    create();
    modelService["retrieveModelVersionFileTree"].mockClear();
    component.editedModelName = "resnet-101";

    component.onSaveModelName();

    expect(modelService["retrieveModelVersionFileTree"]).not.toHaveBeenCalled();
    expect(component.modelName).toBe("resnet-101");
  });

  it("refuses to rename while an upload is in flight", () => {
    modelService["updateModelName"] = vi.fn(() => of({}));
    create();
    component.uploadsInFlight = true;
    component.editedModelName = "resnet-101";

    component.onSaveModelName();

    // The engine captured the old name when the upload started; renaming now would strand its
    // remaining parts, and the abort — which reads the new name — could not clean them up.
    expect(modelService["updateModelName"]).not.toHaveBeenCalled();
    expect(notificationService["error"]).toHaveBeenCalled();
    expect(component.modelName).toBe("resnet-50");
  });

  it("rejects an invalid name without calling the server", () => {
    modelService["updateModelName"] = vi.fn(() => of({}));
    create();
    component.editedModelName = "not a valid name";

    component.onSaveModelName();

    expect(modelService["updateModelName"]).not.toHaveBeenCalled();
    expect(notificationService["error"]).toHaveBeenCalled();
    expect(component.modelName).toBe("resnet-50");
  });

  it("restores the previous description when the update fails", () => {
    modelService["updateModelDescription"] = vi.fn(() => throwError(() => new Error("boom")));
    create();

    component.onModelDescriptionChange("a new description");

    expect(component.modelDescription).toBe("a description");
    expect(notificationService["error"]).toHaveBeenCalled();
  });

  it("skips the description update when nothing changed", () => {
    modelService["updateModelDescription"] = vi.fn(() => of({}));
    create();

    component.onModelDescriptionChange("a description");

    expect(modelService["updateModelDescription"]).not.toHaveBeenCalled();
  });

  it("saves the framework and format, and rolls back a rejected one", () => {
    modelService["updateModelFramework"] = vi.fn(() => of({}));
    modelService["updateModelFormat"] = vi.fn(() => throwError(() => new Error("boom")));
    create();

    component.onFrameworkChange("onnx");
    component.onFormatChange("safetensors");

    expect(modelService["updateModelFramework"]).toHaveBeenCalledWith(MID, "onnx");
    expect(component.modelFramework).toBe("onnx");
    expect(component.modelFormat).toBe("torchscript");
  });

  it("offers exactly the frameworks and formats the backend accepts", () => {
    create();
    const root = openTab("Settings");

    expect(root.querySelectorAll("nz-select").length).toBe(2);
    expect(component.frameworks).toEqual(MODEL_FRAMEWORKS);
    expect(component.formats).toEqual(MODEL_FORMATS);
  });

  it("toggles visibility and downloadability, keeping the switch on the server's answer", () => {
    modelService["updateModelPublicity"] = vi.fn(() => of({}));
    modelService["updateModelDownloadable"] = vi.fn(() => throwError(() => new Error("denied")));
    create();

    component.onPublicStatusChange(true);
    component.onDownloadableStatusChange(false);

    expect(modelService["updateModelPublicity"]).toHaveBeenCalledWith(MID);
    expect(component.modelIsPublic).toBe(true);
    // The rejected toggle leaves the model downloadable, so the switch snaps back.
    expect(component.modelIsDownloadable).toBe(true);
    expect(notificationService["error"]).toHaveBeenCalled();
  });

  it("renders both visibility switches on the Settings tab", () => {
    create();
    const root = openTab("Settings");

    expect(root.querySelectorAll("nz-switch").length).toBe(2);
  });

  it("prefixes the cover path with the selected version and reloads the resolved url", () => {
    modelService["updateModelCoverImage"] = vi.fn(() => of({}));
    modelService["getModelCoverUrl"] = vi.fn(() => of({ url: "http://cover/new.png" }));
    create();
    component.selectedVersion = { mvid: 3, mid: MID, creatorUid: 1, name: "v2" } as any;

    component.onSetCoverImage("images/preview.png");

    expect(modelService["updateModelCoverImage"]).toHaveBeenCalledWith(MID, "v2/images/preview.png");
    expect(component.coverImageUrl).toBe("http://cover/new.png");
  });

  it("does not set a cover while no version is selected", () => {
    modelService["updateModelCoverImage"] = vi.fn(() => of({}));
    create();
    component.selectedVersion = undefined;

    component.onSetCoverImage("images/preview.png");

    expect(modelService["updateModelCoverImage"]).not.toHaveBeenCalled();
  });

  it("collapses and restores the right sider, and maximizes the preview", () => {
    create();
    const root = openTab("Versions & Files");

    expect(root.querySelector("nz-sider")).not.toBeNull();
    component.onClickHideRightBar();
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector("nz-sider")).toBeNull();

    component.onClickScaleTheView();
    fixture.detectChanges();
    expect(component.isMaximized).toBe(true);
    expect(fixture.nativeElement.querySelector(".model-header")).toBeNull();
  });

  // ─── the signed-in user ─────────────────────────────────────────────────────

  it("tracks the signed-in user as the session changes under it", () => {
    // The page is reachable while signed out (a public model), and every service call
    // this component makes picks its endpoint off isLogin.
    create();
    const users = TestBed.inject(UserService) as unknown as StubUserService;
    expect(component.isLogin).toBe(true);

    users.user = undefined;
    users.userChangeSubject.next(undefined);

    expect(component.isLogin).toBe(false);
    expect(component.currentUid).toBeUndefined();

    const signedIn = { uid: 42, name: "n", email: "e", role: Role.REGULAR } as User;
    users.user = signedIn;
    users.userChangeSubject.next(signedIn);

    expect(component.isLogin).toBe(true);
    expect(component.currentUid).toBe(42);
  });

  // ─── the resizable sider ────────────────────────────────────────────────────

  it("stores the dragged sider width on the next animation frame", async () => {
    create();
    expect(component.siderWidth).toBe(400);

    // A drag emits continuously; the component coalesces to one write per frame.
    component.onSideResize({ width: 250, height: 999 });

    // The write is deferred, not immediate — that deferral is the whole point of the
    // requestAnimationFrame hop, and a synchronous assignment would land here.
    expect(component.siderWidth).toBe(400);
    await nextFrame();

    expect(component.siderWidth).toBe(250);
  });

  // ─── guards against an absent model id ──────────────────────────────────────

  it("fetches nothing without a model id", () => {
    create();
    modelService["getModel"].mockClear();
    modelService["retrieveModelVersionList"].mockClear();
    component.mid = undefined;

    component.retrieveModelInfo();
    component.retrieveModelVersionList();

    // Without the guards these would request /api/model/undefined.
    expect(modelService["getModel"]).not.toHaveBeenCalled();
    expect(modelService["retrieveModelVersionList"]).not.toHaveBeenCalled();
  });

  it("saves nothing and changes nothing on screen without a model id", () => {
    modelService["updateModelName"] = vi.fn(() => of({}));
    modelService["updateModelDescription"] = vi.fn(() => of({}));
    modelService["updateModelFramework"] = vi.fn(() => of({}));
    modelService["updateModelFormat"] = vi.fn(() => of({}));
    create();
    component.mid = undefined;
    component.editedModelName = "renamed";

    component.onSaveModelName();
    component.onModelDescriptionChange("changed");
    component.onFrameworkChange("tensorflow");
    component.onFormatChange("onnx");

    expect(modelService["updateModelName"]).not.toHaveBeenCalled();
    expect(modelService["updateModelDescription"]).not.toHaveBeenCalled();
    expect(modelService["updateModelFramework"]).not.toHaveBeenCalled();
    expect(modelService["updateModelFormat"]).not.toHaveBeenCalled();
    // The description, framework and format writes are all optimistic, so a missing guard
    // would also leave the page claiming a value that was never persisted.
    expect(component.modelName).toBe("resnet-50");
    expect(component.modelDescription).toBe("a description");
    expect(component.modelFramework).toBe("pytorch");
    expect(component.modelFormat).toBe("torchscript");
  });

  // ─── timestamps that are not timestamps ─────────────────────────────────────

  it("leaves the creation time blank when the model carries no usable timestamp", () => {
    modelService["getModel"] = vi.fn(() => of(dashboardModel({ model: { creationTime: undefined } })));
    create();

    expect(component.modelCreationTime).toBe("");
    expect(component.modelCreationTimeTooltip).toBe("");

    // A timestamp that arrives as a string is not a timestamp either: formatting it would
    // print a date and a time zone the backend never sent.
    modelService["getModel"] = vi.fn(() => of(dashboardModel({ model: { creationTime: "2023-11-03T00:00:00Z" } })));
    create();

    expect(component.modelCreationTime).toBe("");
    expect(component.modelCreationTimeTooltip).toBe("");
  });

  it("leaves a version's creation time blank, and its row off the sider, without a timestamp", () => {
    modelService["retrieveModelVersionList"] = vi.fn(() =>
      of([{ ...aVersion(1, "v1"), creationTime: undefined as unknown as number }])
    );
    create();

    expect(component.latestVersionCreationTime).toBe("");
    expect(component.selectedVersionCreationTime).toBe("");
    // An em dash here would render "Created at: —"; the absent-timestamp row is meant to
    // disappear instead, and the Model Card supplies its own dash.
    expect(openTab("Versions & Files").querySelector(".version-date")).toBeNull();

    // A timestamp that arrives as a string is not a timestamp either, and this guard tests the
    // type rather than mere presence: a not-undefined check would let the string through and
    // print a date the backend never sent.
    modelService["retrieveModelVersionList"] = vi.fn(() =>
      of([{ ...aVersion(1, "v1"), creationTime: "2023-11-03T00:00:00Z" as unknown as number }])
    );
    create();

    expect(component.latestVersionCreationTime).toBe("");
    expect(component.selectedVersionCreationTime).toBe("");
    expect(openTab("Versions & Files").querySelector(".version-date")).toBeNull();
  });

  // ─── failures reaching the user ─────────────────────────────────────────────

  it("reports a file-tree failure for the version being opened", () => {
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(1, "v1")]));
    modelService["retrieveModelVersionFileTree"] = vi.fn(() => throwError(() => new Error("version tree gone")));
    create();

    expect(notificationService["error"]).toHaveBeenCalledWith("version tree gone");
  });

  it("reports a rejected rename and keeps the old name on screen", () => {
    modelService["updateModelName"] = vi.fn(() => throwError(() => new Error("name already taken")));
    create();
    component.editedModelName = "resnet-101";

    component.onSaveModelName();

    // The rename is not optimistic: the header must not claim a name the server refused.
    expect(component.modelName).toBe("resnet-50");
    expect(notificationService["error"]).toHaveBeenCalledWith("name already taken");
  });

  it("reports a failure refreshing the Model Card after a rename", () => {
    const versions = [aVersion(2, "v2"), aVersion(1, "v1")];
    modelService["updateModelName"] = vi.fn(() => of({}));
    modelService["retrieveModelVersionList"] = vi.fn(() => of(versions));
    create();
    component.onVersionSelected(versions[1]);

    // The browsed version still resolves; only the newest one — fetched for the card — does not.
    modelService["retrieveModelVersionFileTree"] = vi.fn((_mid: number, mvid: number) =>
      mvid === 2 ? throwError(() => new Error("latest tree gone")) : of({ fileNodes: [], size: 0 })
    );
    notificationService["error"].mockClear();
    component.editedModelName = "resnet-101";

    component.onSaveModelName();

    expect(notificationService["error"]).toHaveBeenCalledWith("latest tree gone");
  });

  it("clears the Model Card facts when no version is left to describe", () => {
    const versions = [aVersion(2, "v2"), aVersion(1, "v1")];
    modelService["updateModelName"] = vi.fn(() => of({}));
    modelService["retrieveModelVersionList"] = vi.fn(() => of(versions));
    modelService["retrieveModelVersionFileTree"] = vi.fn(() =>
      of({ fileNodes: [aFile("model.pt", `/model/${OWNER}/resnet-50/v2`)], size: 512 })
    );
    create();
    component.onVersionSelected(versions[1]);
    expect(component.latestVersionSize).toBe(512);

    // Every version was deleted from another tab, so the refresh finds nothing to describe.
    component.versions = [];
    component.editedModelName = "resnet-101";
    component.onSaveModelName();

    // Stale facts here would have the card describing a version that no longer exists.
    expect(component.latestVersionCreationTime).toBe("");
    expect(component.latestVersionFileName).toBe("");
    expect(component.latestVersionSize).toBeUndefined();
  });

  it("falls back to the version's first file when the path being reopened is gone", () => {
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(1, "v1")]));
    modelService["retrieveModelVersionFileTree"] = vi.fn(() =>
      of({
        fileNodes: [
          aFile("first.txt", `/model/${OWNER}/resnet-50/v1`),
          {
            name: "weights",
            type: "directory" as const,
            parentDir: `/model/${OWNER}/resnet-50/v1`,
            children: [aFile("model.pt", `/model/${OWNER}/resnet-50/v1/weights`)],
          },
        ],
        size: 8,
      })
    );
    create();

    component.onVersionSelected(component.versions[0], "weights/deleted.pt");

    // A miss has to read as a miss: returning whatever the walk last looked at would open
    // the nested file instead of the version's first.
    expect(component.currentDisplayedFileName).toBe(`/model/${OWNER}/resnet-50/v1/first.txt`);
  });

  it("treats a cleared description as an empty one", () => {
    modelService["updateModelDescription"] = vi.fn(() => of({}));
    create();

    component.onModelDescriptionChange(undefined as unknown as string);

    // An undefined body would be written into the column verbatim, and the
    // did-anything-change comparison below would then never settle.
    expect(modelService["updateModelDescription"]).toHaveBeenCalledWith(MID, "");
    expect(component.modelDescription).toBe("");
  });

  it("rolls back a rejected framework, and confirms an accepted format", () => {
    modelService["updateModelFramework"] = vi.fn(() => throwError(() => new Error("unknown framework")));
    modelService["updateModelFormat"] = vi.fn(() => of({}));
    create();

    component.onFrameworkChange("tensorflow");

    // The select is written optimistically, so a rejected change has to be put back or the
    // page shows a framework the model does not have.
    expect(component.modelFramework).toBe("pytorch");
    expect(notificationService["error"]).toHaveBeenCalledWith("unknown framework");

    // An accepted framework is confirmed under its own label. The two messages sit twenty
    // lines apart and differ only in the noun, so each needs pinning at its own site — a
    // simultaneous swap of both would otherwise pass for a single one-sided regression.
    modelService["updateModelFramework"] = vi.fn(() => of({}));
    component.onFrameworkChange("tensorflow");

    expect(component.modelFramework).toBe("tensorflow");
    expect(notificationService["success"]).toHaveBeenCalledWith("Framework set to 'tensorflow'");

    component.onFormatChange("onnx");

    expect(component.modelFormat).toBe("onnx");
    expect(notificationService["success"]).toHaveBeenCalledWith("Format set to 'onnx'");
  });

  it("skips a framework or format change that is already the current value", () => {
    modelService["updateModelFramework"] = vi.fn(() => of({}));
    modelService["updateModelFormat"] = vi.fn(() => of({}));
    create();

    component.onFrameworkChange("pytorch");
    component.onFormatChange("torchscript");

    expect(modelService["updateModelFramework"]).not.toHaveBeenCalled();
    expect(modelService["updateModelFormat"]).not.toHaveBeenCalled();
  });

  // ─── the template's own listeners, driven from the DOM ──────────────────────
  //
  // Every test above calls a handler on the instance, which cannot tell whether the
  // template is wired to it at all. These drive the real controls instead.

  const oneVersionWithOneFile = (): void => {
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(1, "v1")]));
    modelService["retrieveModelVersionFileTree"] = vi.fn(() =>
      of({ fileNodes: [aFile("model.pt", `/model/${OWNER}/resnet-50/v1`)], size: 8 })
    );
  };

  /** The toolbar buttons carry no classes, only their tooltip text. */
  const byTooltip = (title: string): HTMLButtonElement => {
    const found = Array.from((fixture.nativeElement as HTMLElement).querySelectorAll<HTMLButtonElement>("button")).find(
      button => button.getAttribute("nz-tooltip") === title
    );
    expect(found, `expected a button tooltipped "${title}"`).toBeDefined();
    return found!;
  };

  it("wires each preview toolbar button to its own handler", () => {
    oneVersionWithOneFile();
    create();
    const root = openTab("Versions & Files");
    const originalClipboardDescriptor = Object.getOwnPropertyDescriptor(navigator, "clipboard");
    const writeText = vi.fn(() => Promise.resolve());
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });

    q<HTMLButtonElement>(root, ".copy-path-btn").click();
    expect(writeText).toHaveBeenCalledWith(`/model/${OWNER}/resnet-50/v1/model.pt`);

    if (originalClipboardDescriptor) {
      Object.defineProperty(navigator, "clipboard", originalClipboardDescriptor);
    } else {
      delete (navigator as any).clipboard;
    }

    byTooltip("Download the file").click();
    expect(downloadService["downloadModelSingleFile"]).toHaveBeenCalledWith(
      `/model/${OWNER}/resnet-50/v1/model.pt`,
      true
    );

    byTooltip("Download this version as ZIP").click();
    expect(downloadService["downloadModelVersion"]).toHaveBeenCalledWith(MID, 1, "resnet-50", "v1");

    // Maximize and Minimize are two buttons behind opposite *ngIfs, one of which is on screen
    // at a time; swapping them would leave the preview with no way back.
    byTooltip("Maximize View").click();
    fixture.detectChanges();
    expect(component.isMaximized).toBe(true);
    byTooltip("Minimize View").click();
    fixture.detectChanges();
    expect(component.isMaximized).toBe(false);

    byTooltip("Hide the right bar").click();
    fixture.detectChanges();
    expect(component.isRightBarCollapsed).toBe(true);
    expect((fixture.nativeElement as HTMLElement).querySelector("nz-sider")).toBeNull();

    byTooltip("Show Tree").click();
    fixture.detectChanges();
    expect(component.isRightBarCollapsed).toBe(false);
    expect((fixture.nativeElement as HTMLElement).querySelector("nz-sider")).not.toBeNull();

    // Both download controls sit behind the same [disabled] expression. Clicking them above
    // only proves what they call; a signed-out viewer, or a model with downloads switched
    // off, must find them dead rather than firing requests the server will refuse.
    render({ isOwner: false, modelIsDownloadable: false, modelIsPublic: true });
    expect(byTooltip("Download the file").disabled).toBe(true);
    expect(q<HTMLButtonElement>(fixture.nativeElement, ".spaced-button").disabled).toBe(true);
  });

  it("wires the sider's resize handle to the width the page keeps", async () => {
    oneVersionWithOneFile();
    create();
    openTab("Versions & Files");

    const sider = fixture.debugElement.query(By.directive(NzResizableDirective));
    expect(sider).not.toBeNull();
    sider.injector.get(NzResizableDirective).nzResize.emit({ width: 275, height: 0 });
    await nextFrame();

    expect(component.siderWidth).toBe(275);
  });

  it("wires the version picker to both the selection and the file tree", () => {
    const versions = [aVersion(2, "v2"), aVersion(1, "v1")];
    modelService["retrieveModelVersionList"] = vi.fn(() => of(versions));
    modelService["retrieveModelVersionFileTree"] = vi.fn((_mid: number, mvid: number) =>
      of({ fileNodes: [aFile(`v${mvid}.pt`, `/model/${OWNER}/resnet-50/v${mvid}`)], size: mvid })
    );
    create();
    const root = openTab("Versions & Files");

    // One select on this tab, and its two-way write-back and its change handler are separate
    // bindings: without the write-back the picker snaps back to the version it was showing.
    expect(root.querySelectorAll("nz-select").length).toBe(1);
    fixture.debugElement.query(By.css("nz-select")).injector.get(NgModel).viewToModelUpdate(versions[1]);
    fixture.detectChanges();

    expect(component.selectedVersion).toBe(versions[1]);
    expect(component.currentDisplayedFileName).toBe(`/model/${OWNER}/resnet-50/v1/v1.pt`);
  });

  it("wires each Settings control to its own field", () => {
    modelService["updateModelName"] = vi.fn(() => of({}));
    modelService["updateModelDescription"] = vi.fn(() => of({}));
    modelService["updateModelFramework"] = vi.fn(() => of({}));
    modelService["updateModelFormat"] = vi.fn(() => of({}));
    create();
    const root = openTab("Settings");

    const nameInput = q<HTMLInputElement>(root, ".settings-name-controls input");
    nameInput.value = "resnet-101";
    nameInput.dispatchEvent(new Event("input"));
    fixture.detectChanges();
    expect(component.editedModelName).toBe("resnet-101");

    const save = Array.from(root.querySelectorAll<HTMLButtonElement>("button")).find(
      button => (button.textContent ?? "").trim() === "Save"
    );
    expect(save, "expected a Save button").toBeDefined();
    save!.click();
    expect(modelService["updateModelName"]).toHaveBeenCalledWith(MID, "resnet-101");

    // Two markdown editors exist at once — the Model Card's read-only one is already
    // instantiated on the default tab — and only the editable one reports changes.
    const editor = fixture.debugElement
      .queryAll(By.directive(MarkdownDescriptionComponent))
      .map(node => node.componentInstance as MarkdownDescriptionComponent)
      .find(instance => instance.editable);
    expect(editor, "expected an editable markdown description").toBeDefined();
    // The editor is fed the description, not some neighbouring string: what a control shows
    // is a separate binding from what its handler writes, and only this reads the former.
    expect(editor!.description).toBe("a description");
    editor!.descriptionChange.emit("a new description");
    expect(modelService["updateModelDescription"]).toHaveBeenCalledWith(MID, "a new description");

    // Framework and format are two identical selects one above the other; a swap between
    // them is invisible on screen and would persist each value into the other's column.
    // Their displayed values need pinning as well as their handlers: exchanging the two
    // [ngModel] inputs alone leaves every handler assertion below intact.
    const selects = fixture.debugElement.queryAll(By.css("nz-select"));
    expect(selects.length).toBe(2);
    expect(selects[0].injector.get(NgModel).model).toBe("pytorch");
    expect(selects[1].injector.get(NgModel).model).toBe("torchscript");
    selects[0].injector.get(NgModel).viewToModelUpdate("tensorflow");
    selects[1].injector.get(NgModel).viewToModelUpdate("onnx");
    fixture.detectChanges();

    expect(modelService["updateModelFramework"]).toHaveBeenCalledWith(MID, "tensorflow");
    expect(modelService["updateModelFormat"]).toHaveBeenCalledWith(MID, "onnx");
  });

  it("wires the version uploader's own outputs to the page's state", () => {
    oneVersionWithOneFile();
    create();
    openTab("Versions & Files");
    const panel = fixture.debugElement.query(By.directive(VersionUploaderComponent))
      .componentInstance as VersionUploaderComponent;
    expect(modelService["retrieveModelVersionList"]).toHaveBeenCalledTimes(1);

    // The panel owns the version flow but not the page's state: without this binding a
    // committed version never reaches the picker until a reload.
    panel.versionCreated.emit();

    expect(modelService["retrieveModelVersionList"]).toHaveBeenCalledTimes(2);

    // And this output is the only thing that arms the rename-during-upload block: unbound,
    // the guard can never engage on a real page however well it is tested on the instance.
    panel.uploadsInFlightChange.emit(true);
    fixture.detectChanges();

    expect(component.uploadsInFlight).toBe(true);
  });
});
