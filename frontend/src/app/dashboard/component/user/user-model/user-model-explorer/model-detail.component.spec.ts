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
import { commonTestImports, commonTestProviders } from "../../../../../common/testing/test-utils";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { StubUserService } from "../../../../../common/service/user/stub-user.service";
import { HubService } from "../../../../../hub/service/hub.service";
import { ModelService } from "../../../../service/user/model/model.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { DatasetFileNode } from "../../../../../common/type/datasetVersionFileTree";
import { ModelVersion } from "../../../../../common/type/model";
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
  let hubService: Record<string, ReturnType<typeof vi.fn>>;
  let notificationService: Record<string, ReturnType<typeof vi.fn>>;

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
    hubService = {
      getCounts: vi.fn(() => of([{ counts: { like: 3 } }])),
      postView: vi.fn(() => of(7)),
      isLiked: vi.fn(() => of([{ isLiked: false }])),
      postLike: vi.fn(() => of(true)),
      postUnlike: vi.fn(() => of(true)),
    };
    notificationService = { success: vi.fn(), error: vi.fn(), info: vi.fn() };

    TestBed.configureTestingModule({
      imports: [ModelDetailComponent, NoopAnimationsModule, ...commonTestImports],
      providers: [
        { provide: ActivatedRoute, useValue: { params: of({ mid: String(MID) }), data: of({}) } },
        { provide: ModelService, useValue: modelService },
        { provide: DownloadService, useValue: downloadService },
        { provide: HubService, useValue: hubService },
        { provide: NotificationService, useValue: notificationService },
        { provide: UserService, useClass: StubUserService },
        { provide: MarkdownService, useValue: { parse: vi.fn(() => "") } },
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

  // ─── loading the model ──────────────────────────────────────────────────────

  it("reads the mid off the route as a number and loads the model", () => {
    create();

    expect(component.mid).toBe(MID);
    expect(modelService["getModel"]).toHaveBeenCalledWith(MID, true);
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
      of({ fileNodes: [aFile("model.pt", `/models/${OWNER}/resnet-50/v2`, 512)], size: 512 })
    );
    create();

    expect(component.selectedVersion).toBe(versions[0]);
    expect(modelService["retrieveModelVersionFileTree"]).toHaveBeenCalledWith(MID, 2, true);
    expect(component.currentDisplayedFileName).toBe(`/models/${OWNER}/resnet-50/v2/model.pt`);
    expect(component.currentFileSize).toBe(512);
    expect(component.currentModelVersionSize).toBe(512);
    expect(component.latestVersionFileName).toBe(`/models/${OWNER}/resnet-50/v2/model.pt`);
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
            parentDir: `/models/${OWNER}/resnet-50/v1`,
            children: [aFile("model.pt", `/models/${OWNER}/resnet-50/v1/weights`)],
          } as DatasetFileNode,
        ],
        size: 128,
      })
    );
    create();

    expect(component.currentDisplayedFileName).toBe(`/models/${OWNER}/resnet-50/v1/weights/model.pt`);
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
        fileNodes: [aFile(`v${mvid}.pt`, `/models/${OWNER}/resnet-50/v${mvid}`)],
        size: mvid * 100,
      })
    );
    create();
    const latestFileName = component.latestVersionFileName;

    component.onVersionSelected(versions[1]);

    expect(component.currentDisplayedFileName).toBe(`/models/${OWNER}/resnet-50/v1/v1.pt`);
    expect(component.currentModelVersionSize).toBe(100);
    // The Model Card still describes the newest version, not the one being browsed.
    expect(component.latestVersionFileName).toBe(latestFileName);
    expect(component.latestVersionSize).toBe(200);
  });

  it("opens the file a tree node points at", () => {
    create();

    component.onVersionFileTreeNodeSelected(aFile("notes.txt", "/models/a/b/v1", 64));

    expect(component.currentDisplayedFileName).toBe("/models/a/b/v1/notes.txt");
    expect(component.currentFileSize).toBe(64);
  });

  // ─── downloads ──────────────────────────────────────────────────────────────

  it("downloads the open file through the authenticated endpoint for an owner", () => {
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(1, "v1")]));
    modelService["retrieveModelVersionFileTree"] = vi.fn(() =>
      of({ fileNodes: [aFile("model.pt", `/models/${OWNER}/resnet-50/v1`)], size: 128 })
    );
    create();

    component.onClickDownloadCurrentFile();

    expect(downloadService["downloadModelSingleFile"]).toHaveBeenCalledWith(
      `/models/${OWNER}/resnet-50/v1/model.pt`,
      true
    );
  });

  it("downloads a public model's file through the anonymous endpoint for a non-owner", () => {
    modelService["getModel"] = vi.fn(() => of(dashboardModel({ isOwner: false, model: { isPublic: true } })));
    modelService["retrieveModelVersionList"] = vi.fn(() => of([aVersion(1, "v1")]));
    modelService["retrieveModelVersionFileTree"] = vi.fn(() =>
      of({ fileNodes: [aFile("model.pt", `/models/${OWNER}/resnet-50/v1`)], size: 128 })
    );
    create();

    component.onClickDownloadCurrentFile();

    expect(downloadService["downloadModelSingleFile"]).toHaveBeenCalledWith(
      `/models/${OWNER}/resnet-50/v1/model.pt`,
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

  // ─── hub stats ──────────────────────────────────────────────────────────────

  it("records a view and loads the like state on open", () => {
    create();

    expect(hubService["postView"]).toHaveBeenCalledWith(MID, expect.any(Number), "model");
    expect(component.viewCount).toBe(7);
    expect(component.likeCount).toBe(3);
    expect(component.isLiked).toBe(false);
  });

  it("toggles a like and refreshes the count", () => {
    create();

    component.toggleLike();

    expect(hubService["postLike"]).toHaveBeenCalledWith(MID, "model");
    expect(component.isLiked).toBe(true);

    component.toggleLike();

    expect(hubService["postUnlike"]).toHaveBeenCalledWith(MID, "model");
    expect(component.isLiked).toBe(false);
  });

  it("leaves the like state alone when the hub rejects the action", () => {
    hubService["postLike"] = vi.fn(() => of(false));
    create();

    component.toggleLike();

    expect(component.isLiked).toBe(false);
  });

  // ─── the file path clipboard ────────────────────────────────────────────────

  it("copies the open file's path and reports failure", async () => {
    create();
    const writeText = vi.fn(() => Promise.resolve());
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });

    render({ currentDisplayedFileName: "/models/a/b/v1/model.pt" });
    await component.copyCurrentFilePath();
    expect(writeText).toHaveBeenCalledWith("/models/a/b/v1/model.pt");
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
      of({ fileNodes: [aFile("notes.txt", `/models/${OWNER}/resnet-50/v1`)], size: 128 })
    );
    create();
    const root = openTab("Versions & Files");

    const renderer = q(root, "texera-user-dataset-file-renderer");
    expect(renderer).toBeTruthy();
    expect(modelService["retrieveModelVersionSingleFile"]).toHaveBeenCalledWith(
      `/models/${OWNER}/resnet-50/v1/notes.txt`,
      true
    );
  });

  it("renders a cover image only once one resolves", () => {
    create();
    expect(fixture.nativeElement.querySelector(".model-cover-image")).toBeNull();

    const root = render({ coverImageUrl: "http://cover" });
    expect(q<HTMLImageElement>(root, ".model-cover-image").src).toContain("http://cover");
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
});
