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

import { ApplicationRef, DebugElement } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { ActivatedRoute, Router } from "@angular/router";
import { concat, of, Subject, throwError } from "rxjs";
import { NzModalService } from "ng-zorro-antd/modal";
import { NzResizableDirective } from "ng-zorro-antd/resizable";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { MarkdownService } from "ngx-markdown";
import { DatasetDetailComponent } from "./dataset-detail.component";
import { DatasetService, MultipartUploadProgress } from "../../../../service/user/dataset/dataset.service";
import { VersionUploaderComponent } from "../../version-uploader/version-uploader.component";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { MOCK_USER, StubUserService } from "../../../../../common/service/user/stub-user.service";
import { ActionType, EntityType, HubService } from "../../../../../hub/service/hub.service";
import { AdminSettingsService } from "../../../../service/admin/settings/admin-settings.service";
import { FileUploadItem } from "../../../../type/dashboard-file.interface";
import { DatasetFileNode, getFullPathFromDatasetFileNode } from "../../../../../common/type/datasetVersionFileTree";
import { DatasetStagedObject } from "../../../../../common/type/dataset-staged-object";
import { commonTestImports, commonTestProviders } from "../../../../../common/testing/test-utils";
import { Contributor, Dataset, DatasetVersion } from "../../../../../common/type/dataset";
import { DashboardDataset } from "../../../../type/dashboard-dataset.interface";
import { HttpErrorResponse, HttpStatusCode } from "@angular/common/http";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { format } from "date-fns";
import { USER_DATASET } from "../../../../../app-routing.constant";

describe("DatasetDetailComponent rendered explorer", () => {
  let fixture: ComponentFixture<DatasetDetailComponent>;
  let component: DatasetDetailComponent;
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [DatasetDetailComponent, ...commonTestImports],
      providers: [
        { provide: ActivatedRoute, useValue: { params: of({ did: 1 }), data: of({}) } },
        { provide: NzModalService, useValue: {} },
        {
          provide: DatasetService,
          useValue: {
            getDataset: vi.fn(() =>
              of({
                dataset: { name: "test-dataset", description: "", isPublic: false, isDownloadable: true },
                accessPrivilege: "WRITE",
                ownerEmail: "owner@texera.com",
                isOwner: true,
              })
            ),
            retrieveDatasetVersionList: vi.fn(() => of([])),
            retrieveDatasetLatestVersion: vi.fn(() =>
              of({
                dvid: 1,
                did: 1,
                creatorUid: 1,
                name: "v1",
                versionHash: undefined,
                creationTime: undefined,
                fileNodes: [],
              })
            ),
            retrieveDatasetVersionFileTree: vi.fn(() => of({ fileNodes: [], size: 1024 })),
            getDatasetDiff: vi.fn(() => of([])),
            createDatasetVersion: vi.fn(() => of({})),
            deleteDatasetFile: vi.fn(() => of({})),
          },
        },
        { provide: NotificationService, useValue: { success: vi.fn(), error: vi.fn(), info: vi.fn() } },
        { provide: DownloadService, useValue: {} },
        { provide: UserService, useClass: StubUserService },
        {
          provide: HubService,
          useValue: {
            getCounts: vi.fn(() => of([{ counts: { like: 0 } }])),
            postView: vi.fn(() => of(0)),
            isLiked: vi.fn(() => of([{ isLiked: false }])),
          },
        },
        { provide: AdminSettingsService, useValue: { getPublicSetting: vi.fn(() => of("3")) } },
        { provide: MarkdownService, useValue: { parse: vi.fn(() => "") } },
        ...commonTestProviders,
      ],
    });

    fixture = TestBed.createComponent(DatasetDetailComponent);
    component = fixture.componentInstance;
    // Log in so ngOnInit reaches loadUploadSettings (maxConcurrentFiles = 3).
    (TestBed.inject(UserService) as unknown as StubUserService).userChangeSubject.next(MOCK_USER);
    fixture.detectChanges();
  });

  /**
   * A failed upload has to tell the user why, mark the task failed without leaving its bar at
   * 100%, and free the concurrency slot — otherwise the queue stalls behind a dead upload.
   */
  /**
   * The explorer's toolbar and upload panel are template-only: whether a download is offered at all,
   * which of the maximize/minimize pair is showing, and what an in-flight upload reports. The suite
   * around this one drives component state and never asserts on what is rendered.
   */
  describe("rendered explorer", () => {
    /**
     * Applies some state and renders the "Versions & Files" tab. nz-tabs only instantiates the
     * active tab, and the toolbar under test lives in the second one, so it has to be selected
     * before anything in it exists to assert on.
     */
    function render(setup: (c: DatasetDetailComponent) => void = () => {}): HTMLElement {
      setup(component);
      fixture.detectChanges();
      const host = fixture.nativeElement as HTMLElement;
      const tabButtons = host.querySelectorAll<HTMLElement>(".ant-tabs-tab-btn");
      const versionsTab = Array.from(tabButtons).find(tab => tab.textContent?.includes("Versions & Files"));
      if (versionsTab && !versionsTab.closest(".ant-tabs-tab")?.classList.contains("ant-tabs-tab-active")) {
        versionsTab.click();
        fixture.detectChanges();
      }
      return host;
    }

    /** The button carrying the given tooltip, or undefined. */
    function byTooltip(title: string): HTMLButtonElement | undefined {
      return Array.from((fixture.nativeElement as HTMLElement).querySelectorAll<HTMLButtonElement>("button")).find(
        b => b.getAttribute("nz-tooltip") === title
      );
    }

    const aVersion = { dvid: 1, did: 1, creatorUid: 1, name: "v1" } as any;

    describe("download gating", () => {
      it("offers the file download to a logged-in user who is allowed to download", () => {
        render(c => {
          c.selectedVersion = aVersion;
          c.isLogin = true;
          vi.spyOn(c, "isDownloadAllowed").mockReturnValue(true);
        });

        expect(byTooltip("Download the file")!.disabled).toBe(false);
      });

      it("withholds it from a signed-out visitor", () => {
        render(c => {
          c.selectedVersion = aVersion;
          c.isLogin = false;
          vi.spyOn(c, "isDownloadAllowed").mockReturnValue(true);
        });

        expect(byTooltip("Download the file")!.disabled).toBe(true);
      });

      it("withholds it when the dataset is not downloadable", () => {
        // Both halves of the guard matter: being signed in is not on its own permission to take
        // a copy of someone else's non-downloadable dataset.
        render(c => {
          c.selectedVersion = aVersion;
          c.isLogin = true;
          vi.spyOn(c, "isDownloadAllowed").mockReturnValue(false);
        });

        expect(byTooltip("Download the file")!.disabled).toBe(true);
      });

      it("applies the same rule to the whole-version download", () => {
        render(c => {
          c.selectedVersion = aVersion;
          c.isLogin = true;
          vi.spyOn(c, "isDownloadAllowed").mockReturnValue(false);
        });

        expect(byTooltip("Download Dataset")!.disabled).toBe(true);
      });

      it("offers no download at all until a version is selected", () => {
        render(c => {
          c.selectedVersion = undefined;
          c.isLogin = true;
        });

        expect(byTooltip("Download the file")).toBeUndefined();
        expect(byTooltip("Download Dataset")).toBeUndefined();
      });
    });

    describe("view size toggle", () => {
      it("offers only Maximize while the view is normal", () => {
        render(c => {
          c.selectedVersion = aVersion;
          c.isMaximized = false;
        });

        expect(byTooltip("Maximize View")).toBeDefined();
        expect(byTooltip("Minimize View")).toBeUndefined();
      });

      it("offers only Minimize once the view is maximized", () => {
        // Showing both, or the wrong one, leaves the user with no way back.
        render(c => {
          c.selectedVersion = aVersion;
          c.isMaximized = true;
        });

        expect(byTooltip("Minimize View")).toBeDefined();
        expect(byTooltip("Maximize View")).toBeUndefined();
      });
    });

    describe("file heading", () => {
      it("offers the copy-path control only once a file is on screen", () => {
        const el = render(c => (c.currentDisplayedFileName = ""));
        expect(el.querySelector(".copy-path-btn")).toBeNull();

        render(c => (c.currentDisplayedFileName = "a/b.csv"));
        expect((fixture.nativeElement as HTMLElement).querySelector(".copy-path-btn")).not.toBeNull();
      });

      it("copies the path of the file being shown", () => {
        const spy = vi.spyOn(component, "copyCurrentFilePath").mockResolvedValue(undefined);
        const el = render(c => (c.currentDisplayedFileName = "a/b.csv"));

        el.querySelector<HTMLElement>(".copy-path-btn")!.click();

        expect(spy).toHaveBeenCalledTimes(1);
      });

      it("shows the file size in human units, and nothing when it is unknown", () => {
        const el = render(c => {
          c.currentDisplayedFileName = "a/b.csv";
          c.currentFileSize = 2048;
        });
        expect(el.querySelector(".file-size")?.textContent).toContain("2");

        render(c => (c.currentFileSize = undefined));
        expect((fixture.nativeElement as HTMLElement).querySelector(".file-size")).toBeNull();
      });
    });

    describe("version details", () => {
      it("reports the version size and creation time once a version is chosen", () => {
        const el = render(c => {
          c.selectedVersion = aVersion;
          c.currentDatasetVersionSize = 1024;
          c.selectedVersionCreationTime = "2026-01-02 03:04";
        });

        expect(el.querySelector(".version-size")?.textContent).toContain("Version Size:");
        expect(el.querySelector(".version-date")?.textContent).toContain("2026-01-02 03:04");
      });

      it("hides the creation time when the version has none", () => {
        const el = render(c => {
          c.selectedVersion = aVersion;
          c.selectedVersionCreationTime = "";
        });

        expect(el.querySelector(".version-date")).toBeNull();
      });
    });

    it("stages a file deletion and lets the panel count it", () => {
      render(); // the panel lives in the Versions & Files tab, which nz-tabs renders lazily
      const node: DatasetFileNode = { name: "a.txt", type: "file", parentDir: "/owner@texera.com/test-dataset/v1" };

      component.onPreviouslyUploadedFileDeleted(node);

      const panel = fixture.debugElement.query(By.directive(VersionUploaderComponent))
        .componentInstance as VersionUploaderComponent;
      expect(panel.pendingChangesCount).toBe(1);
    });

    it("reloads the version list once the panel reports a new version", () => {
      render(); // the panel lives in the Versions & Files tab, which nz-tabs renders lazily
      const datasetService = TestBed.inject(DatasetService) as unknown as Record<string, ReturnType<typeof vi.fn>>;
      const before = datasetService["retrieveDatasetVersionList"].mock.calls.length;
      const panel = fixture.debugElement.query(By.directive(VersionUploaderComponent))
        .componentInstance as VersionUploaderComponent;

      panel.versionCreated.emit();

      // The panel owns the version flow but not the page's state: without this binding the new
      // version is committed and never appears in the picker until a reload.
      expect(datasetService["retrieveDatasetVersionList"].mock.calls.length).toBe(before + 1);
    });
  });

  describe("contributor cards", () => {
    const full: Contributor = {
      name: "Contributor A",
      creator: true,
      affiliation: "Test Lab",
      email: "contributor-a@test.com",
      comments: "notes",
    };
    const blank: Contributor = { name: "Contributor B", creator: false };

    beforeEach(() => {
      component.datasetContributors = [full, blank];
      component.userDatasetAccessLevel = "WRITE";
      fixture.detectChanges();
    });

    it("renders one card per contributor with values, a creator star, and dashes for blanks", () => {
      const cards: NodeListOf<HTMLElement> = fixture.nativeElement.querySelectorAll(".contributor-card");
      expect(cards.length).toBe(2);

      expect(cards[0].querySelector(".contributor-name")?.textContent).toContain("Contributor A");
      expect(cards[0].querySelector(".creator-star")).not.toBeNull();
      expect(cards[0].textContent).toContain("contributor-a@test.com");

      expect(cards[1].querySelector(".creator-star")).toBeNull();
      const blankValues: NodeListOf<HTMLElement> = cards[1].querySelectorAll(".contributor-value.empty");
      expect(blankValues.length).toBe(3);
      blankValues.forEach(value => expect(value.textContent?.trim()).toBe("—"));
    });

    it("shows edit controls only with write access", () => {
      expect(fixture.nativeElement.querySelector(".contributor-actions")).not.toBeNull();
      expect(fixture.nativeElement.querySelector(".contributor-card-add")).not.toBeNull();

      component.userDatasetAccessLevel = "READ";
      fixture.detectChanges();

      expect(fixture.nativeElement.querySelector(".contributor-actions")).toBeNull();
      expect(fixture.nativeElement.querySelector(".contributor-card-add")).toBeNull();
    });

    it("starts adding a contributor when the add tile is clicked", () => {
      const onAdd = vi.spyOn(component, "onAddContributor").mockImplementation(() => {});

      (fixture.nativeElement.querySelector(".contributor-card-add") as HTMLElement).click();

      expect(onAdd).toHaveBeenCalledTimes(1);
    });
  });
});

describe("DatasetDetailComponent behavior", () => {
  let fixture: ComponentFixture<DatasetDetailComponent>;
  let component: DatasetDetailComponent;

  type MockService = Record<string, ReturnType<typeof vi.fn>>;
  let datasetServiceStub: MockService;
  let notificationServiceStub: MockService;
  let downloadServiceStub: MockService;
  let hubServiceStub: MockService;
  let adminSettingsServiceStub: MockService;
  let modalServiceStub: MockService;

  const CREATION_TS = 1_700_000_000_000;

  const makeDataset = (overrides: Partial<Dataset> = {}): Dataset => ({
    did: 5,
    ownerUid: 9,
    name: "ds",
    isPublic: false,
    isDownloadable: true,
    storagePath: undefined,
    description: "desc",
    creationTime: undefined,
    coverImage: undefined,
    ...overrides,
  });

  const makeDashboardDataset = (overrides: Partial<DashboardDataset> = {}): DashboardDataset => ({
    isOwner: false,
    ownerEmail: "owner@texera.com",
    dataset: makeDataset(),
    accessPrivilege: "NONE",
    size: 0,
    ...overrides,
  });

  const makeVersion = (overrides: Partial<DatasetVersion> = {}): DatasetVersion => ({
    dvid: 1,
    did: 5,
    creatorUid: 9,
    name: "v1",
    versionHash: undefined,
    creationTime: undefined,
    fileNodes: undefined,
    ...overrides,
  });

  const fileLeaf = (name: string, parentDir: string, size: number): DatasetFileNode => ({
    name,
    type: "file",
    parentDir,
    size,
  });

  const createComponent = (params: Record<string, unknown> = { did: 5 }): void => {
    TestBed.configureTestingModule({
      imports: [DatasetDetailComponent, ...commonTestImports],
      providers: [
        { provide: ActivatedRoute, useValue: { params: of(params), data: of({}) } },
        { provide: NzModalService, useValue: modalServiceStub },
        { provide: DatasetService, useValue: datasetServiceStub },
        { provide: NotificationService, useValue: notificationServiceStub },
        { provide: DownloadService, useValue: downloadServiceStub },
        { provide: UserService, useClass: StubUserService },
        { provide: HubService, useValue: hubServiceStub },
        { provide: AdminSettingsService, useValue: adminSettingsServiceStub },
        { provide: MarkdownService, useValue: { parse: vi.fn(() => "") } },
        ...commonTestProviders,
      ],
    });
    fixture = TestBed.createComponent(DatasetDetailComponent);
    component = fixture.componentInstance;
  };

  // The StubUserService emits MOCK_USER in its own constructor, before the
  // component subscribes, so currentUid starts undefined; re-emit to log in.
  const login = (): void => {
    (TestBed.inject(UserService) as unknown as StubUserService).userChangeSubject.next(MOCK_USER);
  };

  beforeEach(() => {
    datasetServiceStub = {
      getDataset: vi.fn(() => of(makeDashboardDataset())),
      retrieveDatasetVersionList: vi.fn(() => of([])),
      retrieveDatasetLatestVersion: vi.fn(() => of(makeVersion())),
      getDatasetCoverUrl: vi.fn(() => of({ url: "http://cover" })),
      retrieveDatasetVersionFileTree: vi.fn(() => of({ fileNodes: [fileLeaf("a.txt", "/root", 1)], size: 1 })),
      createDatasetVersion: vi.fn(() => of(makeVersion())),
      updateDatasetPublicity: vi.fn(() => of({})),
      updateDatasetDownloadable: vi.fn(() => of({})),
      updateDatasetCoverImage: vi.fn(() => of({})),
      updateDatasetDescription: vi.fn(() => of({})),
      updateDatasetContributors: vi.fn(() => of(undefined)),
      updateDatasetName: vi.fn(() => of({})),
      deleteDatasets: vi.fn(() => of({})),
      deleteDatasetFile: vi.fn(() => of({})),
      getDatasetDiff: vi.fn(() => of([])),
      multipartUpload: vi.fn(() => of()),
      finalizeMultipartUpload: vi.fn(() => of({})),
    };
    notificationServiceStub = { success: vi.fn(), error: vi.fn(), info: vi.fn() };
    modalServiceStub = { create: vi.fn() };
    downloadServiceStub = {
      downloadDatasetVersion: vi.fn(() => of(new Blob())),
      downloadSingleFile: vi.fn(() => of(new Blob())),
    };
    hubServiceStub = {
      getCounts: vi.fn(() => of([{ counts: { like: 0 } }])),
      postView: vi.fn(() => of(0)),
      isLiked: vi.fn(() => of([{ isLiked: false }])),
      postLike: vi.fn(() => of(true)),
      postUnlike: vi.fn(() => of(true)),
    };
    adminSettingsServiceStub = { getPublicSetting: vi.fn(() => of("50")) };
  });

  describe("ngOnInit", () => {
    it("loads info, versions, like and view counts but skips liked/upload settings without a current user", () => {
      hubServiceStub.getCounts.mockReturnValue(of([{ counts: { like: 7 } }]));
      hubServiceStub.postView.mockReturnValue(of(42));

      createComponent({ did: 5 });
      // Drive the genuine logged-out path rather than relying on StubUserService's emission
      // quirk (its user makes isLogin default to true).
      component.isLogin = false;
      fixture.detectChanges();

      expect(datasetServiceStub.getDataset).toHaveBeenCalled();
      expect(datasetServiceStub.retrieveDatasetVersionList).toHaveBeenCalled();
      expect(datasetServiceStub.retrieveDatasetLatestVersion).toHaveBeenCalled();
      expect(component.likeCount).toBe(7);
      expect(component.viewCount).toBe(42);
      expect(hubServiceStub.isLiked).not.toHaveBeenCalled();
    });

    it("fetches liked status for a logged-in user", () => {
      hubServiceStub.isLiked.mockReturnValue(of([{ isLiked: true }]));

      createComponent({ did: 5 });
      login();
      fixture.detectChanges();

      expect(hubServiceStub.isLiked).toHaveBeenCalled();
      expect(component.isLiked).toBe(true);
    });

    it("makes no hub calls when the route carries no did", () => {
      createComponent({});
      component.ngOnInit();

      expect(datasetServiceStub.getDataset).not.toHaveBeenCalled();
      expect(hubServiceStub.getCounts).not.toHaveBeenCalled();
      expect(hubServiceStub.postView).not.toHaveBeenCalled();
    });

    it("reads a counts response with no like tally as no likes, and no liked record as not liked", () => {
      // Both are legitimate wire shapes: `counts` is a partial map keyed by action
      // type, and `isLiked` simply omits entities the user has no record against.
      hubServiceStub.getCounts.mockReturnValue(of([{ counts: {} }]));
      hubServiceStub.isLiked.mockReturnValue(of([]));

      createComponent({ did: 5 });
      // Seed both fields with values the response cannot produce, so falling back
      // is distinguishable from leaving whatever happened to be there.
      component.likeCount = 9;
      component.isLiked = true;
      login();
      fixture.detectChanges();

      const likeTag: HTMLElement = fixture.nativeElement.querySelector(".like-tag");
      expect(likeTag).not.toBeNull();
      expect((likeTag.textContent ?? "").trim()).toBe("0");
      expect(likeTag.classList).not.toContain("liked");
      // The tally shown is the dataset's own like count, not some other entity's
      // or some other action's: nothing else in the suite pins these arguments.
      expect(hubServiceStub.getCounts).toHaveBeenCalledWith([EntityType.Dataset], [5], [ActionType.Like]);
    });
  });

  describe("retrieveDatasetInfo", () => {
    it("maps dataset fields, formats numeric creation time, and resolves the cover image url", () => {
      const dashboard = makeDashboardDataset({
        isOwner: true,
        ownerEmail: "o@e.com",
        accessPrivilege: "WRITE",
        dataset: makeDataset({
          name: "N",
          description: "D",
          isPublic: true,
          isDownloadable: false,
          coverImage: "cover.png",
          creationTime: CREATION_TS,
        }),
      });
      datasetServiceStub.getDataset.mockReturnValue(of(dashboard));
      datasetServiceStub.getDatasetCoverUrl.mockReturnValue(of({ url: "http://c" }));

      createComponent();
      component.did = 5;
      component.retrieveDatasetInfo();

      expect(component.datasetName).toBe("N");
      expect(component.datasetDescription).toBe("D");
      expect(component.userDatasetAccessLevel).toBe("WRITE");
      expect(component.datasetIsPublic).toBe(true);
      expect(component.datasetIsDownloadable).toBe(false);
      expect(component.ownerEmail).toBe("o@e.com");
      expect(component.isOwner).toBe(true);
      expect(component.coverImageUrl).toBe("http://c");
      expect(component.datasetCreationTime).toEqual(format(new Date(CREATION_TS), "MM/dd/yyyy HH:mm:ss"));
      expect(component.datasetCreationTime).toMatch(/^\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}:\d{2}$/);
    });

    it("nulls the cover image url when its retrieval fails", () => {
      datasetServiceStub.getDataset.mockReturnValue(
        of(makeDashboardDataset({ dataset: makeDataset({ coverImage: "c.png" }) }))
      );
      datasetServiceStub.getDatasetCoverUrl.mockReturnValue(throwError(() => new Error("boom")));

      createComponent();
      component.did = 5;
      component.coverImageUrl = "stale";
      component.retrieveDatasetInfo();

      expect(component.coverImageUrl).toBeNull();
    });

    it("leaves the cover image url null and skips the cover fetch when there is no cover image", () => {
      datasetServiceStub.getDataset.mockReturnValue(
        of(makeDashboardDataset({ dataset: makeDataset({ coverImage: undefined }) }))
      );

      createComponent();
      component.did = 5;
      component.coverImageUrl = "stale";
      component.retrieveDatasetInfo();

      expect(component.coverImageUrl).toBeNull();
      expect(datasetServiceStub.getDatasetCoverUrl).not.toHaveBeenCalled();
    });

    /**
     * Stands in for the platform time-zone formatter so the assertions do not depend on
     * whichever zone the machine running the suite sits in. `formatted` maps the requested
     * `timeZoneName` option to the whole string the formatter would return, so the stub
     * answers "long" and "short" differently the way a real formatter does — asking for the
     * wrong one stays observable. Any call that does not ask for a zone name is delegated to
     * the real constructor, since other code formats the same date through Intl.
     */
    const stubZoneFormatter = (formatted: Record<string, string>) => {
      const realDateTimeFormat = Intl.DateTimeFormat;
      return vi.spyOn(Intl, "DateTimeFormat").mockImplementation(function (locale?: any, options?: any) {
        const requested = options?.timeZoneName as string | undefined;
        return requested === undefined
          ? new (realDateTimeFormat as any)(locale, options)
          : ({ format: () => formatted[requested] ?? `<unstubbed:${requested}>` } as any);
      } as any);
    };

    const renderTooltipWithCreationTime = () => {
      datasetServiceStub.getDataset.mockReturnValue(
        of(makeDashboardDataset({ dataset: makeDataset({ creationTime: CREATION_TS }) }))
      );

      createComponent();
      component.did = 5;
      component.retrieveDatasetInfo();
    };

    it("takes the tooltip's time zone from the spelled-out name the formatter appends", () => {
      // The parenthetical is the segment after the last ", " of a long-form formatted
      // date. Reading any other segment, or asking the formatter for the abbreviated
      // zone, would put "11/14/2023" or "PST" in front of the user instead.
      const zoned = stubZoneFormatter({
        long: "11/14/2023, Pacific Standard Time",
        short: "11/14/2023, PST",
      });

      try {
        renderTooltipWithCreationTime();

        expect(component.datasetCreationTimeTooltip).toMatch(/ \(Pacific Standard Time\)$/);
      } finally {
        // Vitest runs these specs without isolation, so a leaked global spy would
        // follow the worker into the next spec file.
        zoned.mockRestore();
      }
    });

    it("leaves the tooltip's time zone empty when the runtime supplies no zone name", () => {
      // A formatter that yields no zone name at all must render an empty parenthetical
      // rather than leaking "undefined" into a user-visible tooltip.
      const zoneless = stubZoneFormatter({ long: "", short: "" });

      try {
        renderTooltipWithCreationTime();

        expect(component.datasetCreationTimeTooltip).toMatch(/ \(\)$/);
      } finally {
        zoneless.mockRestore();
      }
    });
  });

  describe("retrieveDatasetVersionList", () => {
    it("selects the first version and delegates to onVersionSelected when the list is non-empty", () => {
      const v1 = makeVersion({ dvid: 10, name: "v10" });
      const v2 = makeVersion({ dvid: 9, name: "v9" });
      datasetServiceStub.retrieveDatasetVersionList.mockReturnValue(of([v1, v2]));

      createComponent();
      component.did = 5;
      const spy = vi.spyOn(component, "onVersionSelected");
      component.retrieveDatasetVersionList();

      expect(component.versions).toEqual([v1, v2]);
      expect(component.selectedVersion).toEqual(v1);
      expect(spy).toHaveBeenCalledWith(v1);
    });

    it("makes no selection when the version list is empty", () => {
      datasetServiceStub.retrieveDatasetVersionList.mockReturnValue(of([]));

      createComponent();
      component.did = 5;
      component.selectedVersion = undefined;
      const spy = vi.spyOn(component, "onVersionSelected");
      component.retrieveDatasetVersionList();

      expect(component.versions).toEqual([]);
      expect(component.selectedVersion).toBeUndefined();
      expect(spy).not.toHaveBeenCalled();
    });
  });

  describe("onVersionSelected", () => {
    it("walks nested directories to the first file leaf and loads it", () => {
      const leaf = fileLeaf("c.txt", "/root/a", 42);
      const tree: DatasetFileNode[] = [{ name: "a", type: "directory", parentDir: "/root", children: [leaf] }];
      datasetServiceStub.retrieveDatasetVersionFileTree.mockReturnValue(of({ fileNodes: tree, size: 100 }));

      createComponent();
      component.did = 5;
      component.onVersionSelected(makeVersion({ dvid: 2, creationTime: CREATION_TS }));

      expect(component.fileTreeNodeList).toEqual(tree);
      expect(component.currentDatasetVersionSize).toBe(100);
      expect(component.currentDisplayedFileName).toBe(getFullPathFromDatasetFileNode(leaf));
      expect(component.currentFileSize).toBe(42);
      expect(component.selectedVersionCreationTime).toMatch(/^\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}:\d{2}$/);
    });

    it("does not fetch a file tree for a version without a dvid", () => {
      createComponent();
      component.did = 5;
      component.onVersionSelected(makeVersion({ dvid: undefined }));

      expect(datasetServiceStub.retrieveDatasetVersionFileTree).not.toHaveBeenCalled();
    });

    it("does not throw and leaves the displayed file untouched when the version has no files", () => {
      datasetServiceStub.retrieveDatasetVersionFileTree.mockReturnValue(of({ fileNodes: [], size: 0 }));

      createComponent();
      component.did = 5;
      component.currentDisplayedFileName = "stale.txt";
      component.currentFileSize = 99;

      expect(() => component.onVersionSelected(makeVersion({ dvid: 2 }))).not.toThrow();

      expect(component.fileTreeNodeList).toEqual([]);
      expect(component.currentDatasetVersionSize).toBe(0);
      expect(component.currentDisplayedFileName).toBe("stale.txt");
      expect(component.currentFileSize).toBe(99);
    });
  });

  describe("retrieveLatestVersionFile", () => {
    it("fetches the latest version independently and sets latestVersionFileName to the first leaf file", () => {
      const leaf = fileLeaf("b.txt", "/root", 7);
      datasetServiceStub.retrieveDatasetLatestVersion.mockReturnValue(of(makeVersion({ fileNodes: [leaf] })));

      createComponent();
      component.did = 5;
      component.retrieveLatestVersionFile();

      expect(datasetServiceStub.retrieveDatasetLatestVersion).toHaveBeenCalledWith(5);
      expect(component.latestVersionFileName).toBe(getFullPathFromDatasetFileNode(leaf));
    });

    it("walks nested directories to find the first leaf file", () => {
      const leaf = fileLeaf("c.txt", "/root/a", 3);
      const tree: DatasetFileNode[] = [{ name: "a", type: "directory", parentDir: "/root", children: [leaf] }];
      datasetServiceStub.retrieveDatasetLatestVersion.mockReturnValue(of(makeVersion({ fileNodes: tree })));

      createComponent();
      component.did = 5;
      component.retrieveLatestVersionFile();

      expect(component.latestVersionFileName).toBe(getFullPathFromDatasetFileNode(leaf));
    });

    it("sets latestVersionFileName to an empty string when the latest version has no files", () => {
      datasetServiceStub.retrieveDatasetLatestVersion.mockReturnValue(of(makeVersion()));

      createComponent();
      component.did = 5;
      component.retrieveLatestVersionFile();

      expect(component.latestVersionFileName).toBe("");
    });

    it("derives latestVersionCreationTime from the latest version's creationTime", () => {
      datasetServiceStub.retrieveDatasetLatestVersion.mockReturnValue(
        of(makeVersion({ dvid: 3, creationTime: CREATION_TS }))
      );

      createComponent();
      component.did = 5;
      component.retrieveLatestVersionFile();

      expect(component.latestVersionCreationTime).toEqual(format(new Date(CREATION_TS), "MM/dd/yyyy HH:mm:ss"));
    });

    it("leaves latestVersionCreationTime empty when the latest version has no creation time", () => {
      datasetServiceStub.retrieveDatasetLatestVersion.mockReturnValue(of(makeVersion({ creationTime: undefined })));

      createComponent();
      component.did = 5;
      component.retrieveLatestVersionFile();

      expect(component.latestVersionCreationTime).toBe("");
    });

    it("sets latestVersionSize from a file-tree fetch for the latest version's dvid", () => {
      datasetServiceStub.retrieveDatasetLatestVersion.mockReturnValue(of(makeVersion({ dvid: 7 })));
      datasetServiceStub.retrieveDatasetVersionFileTree.mockReturnValue(of({ fileNodes: [], size: 4096 }));

      createComponent();
      component.did = 5;
      component.retrieveLatestVersionFile();

      expect(datasetServiceStub.retrieveDatasetVersionFileTree).toHaveBeenCalledWith(5, 7, expect.anything());
      expect(component.latestVersionSize).toBe(4096);
    });

    it("does not fetch a size when the latest version has no dvid", () => {
      datasetServiceStub.retrieveDatasetLatestVersion.mockReturnValue(of(makeVersion({ dvid: undefined })));

      createComponent();
      component.did = 5;
      component.retrieveLatestVersionFile();

      expect(datasetServiceStub.retrieveDatasetVersionFileTree).not.toHaveBeenCalled();
      expect(component.latestVersionSize).toBeUndefined();
    });

    it("clears a previously fetched latestVersionSize when the latest version has no dvid", () => {
      datasetServiceStub.retrieveDatasetLatestVersion.mockReturnValue(of(makeVersion({ dvid: 7 })));
      datasetServiceStub.retrieveDatasetVersionFileTree.mockReturnValue(of({ fileNodes: [], size: 4096 }));

      createComponent();
      component.did = 5;
      component.retrieveLatestVersionFile();

      expect(component.latestVersionSize).toBe(4096);

      // Without a dvid there is no size to show, so the stale one must not linger.
      datasetServiceStub.retrieveDatasetLatestVersion.mockReturnValue(of(makeVersion({ dvid: undefined })));
      component.retrieveLatestVersionFile();

      expect(component.latestVersionSize).toBeUndefined();
    });

    it("ignores a superseded call's size response that resolves after a newer one", () => {
      // The first call's file-tree request never completes before the second starts.
      const pendingTree = new Subject<{ fileNodes: DatasetFileNode[]; size: number }>();
      datasetServiceStub.retrieveDatasetLatestVersion.mockReturnValue(of(makeVersion({ dvid: 7 })));
      datasetServiceStub.retrieveDatasetVersionFileTree.mockReturnValue(pendingTree);

      createComponent();
      component.did = 5;
      component.retrieveLatestVersionFile();

      expect(component.latestVersionSize).toBeUndefined();

      // A second call supersedes the first and resolves immediately.
      datasetServiceStub.retrieveDatasetVersionFileTree.mockReturnValue(of({ fileNodes: [], size: 200 }));
      component.retrieveLatestVersionFile();

      expect(component.latestVersionSize).toBe(200);

      // The superseded response arriving late must not overwrite the fresher size.
      pendingTree.next({ fileNodes: [], size: 999 });

      expect(component.latestVersionSize).toBe(200);
    });

    it("keeps the latest-version facts fixed when a different version is later selected", () => {
      datasetServiceStub.retrieveDatasetLatestVersion.mockReturnValue(
        of(makeVersion({ dvid: 10, creationTime: CREATION_TS }))
      );
      datasetServiceStub.retrieveDatasetVersionFileTree.mockReturnValue(of({ fileNodes: [], size: 500 }));

      createComponent();
      component.did = 5;
      component.retrieveLatestVersionFile();

      expect(component.latestVersionSize).toBe(500);
      expect(component.latestVersionCreationTime).toEqual(format(new Date(CREATION_TS), "MM/dd/yyyy HH:mm:ss"));

      // Selecting an older version updates only the selection-scoped values; the
      // Data Card's latest-version facts stay pinned to the latest version.
      datasetServiceStub.retrieveDatasetVersionFileTree.mockReturnValue(of({ fileNodes: [], size: 99 }));
      component.onVersionSelected(makeVersion({ dvid: 9, creationTime: CREATION_TS - 1000 }));

      expect(component.currentDatasetVersionSize).toBe(99);
      expect(component.selectedVersionCreationTime).toEqual(
        format(new Date(CREATION_TS - 1000), "MM/dd/yyyy HH:mm:ss")
      );
      expect(component.latestVersionSize).toBe(500);
      expect(component.latestVersionCreationTime).toEqual(format(new Date(CREATION_TS), "MM/dd/yyyy HH:mm:ss"));
    });

    it("does nothing when there is no did", () => {
      createComponent();
      component.did = undefined;
      component.retrieveLatestVersionFile();

      expect(datasetServiceStub.retrieveDatasetLatestVersion).not.toHaveBeenCalled();
    });
  });

  describe("isDownloadAllowed and userHasWriteAccess", () => {
    beforeEach(() => createComponent());

    it("always allows the owner to download, even when the dataset is not downloadable", () => {
      component.isOwner = true;
      component.datasetIsDownloadable = false;
      expect(component.isDownloadAllowed()).toBe(true);
    });

    it("allows a non-owner to download a public downloadable dataset without explicit access", () => {
      component.isOwner = false;
      component.datasetIsDownloadable = true;
      component.datasetIsPublic = true;
      component.userDatasetAccessLevel = "NONE";
      expect(component.isDownloadAllowed()).toBe(true);
    });

    it("blocks a non-owner from a private downloadable dataset without access", () => {
      component.isOwner = false;
      component.datasetIsDownloadable = true;
      component.datasetIsPublic = false;
      component.userDatasetAccessLevel = "NONE";
      expect(component.isDownloadAllowed()).toBe(false);
    });

    it("blocks download when the dataset is not downloadable", () => {
      component.isOwner = false;
      component.datasetIsDownloadable = false;
      component.datasetIsPublic = true;
      expect(component.isDownloadAllowed()).toBe(false);
    });

    it("reports write access only for the WRITE privilege", () => {
      component.userDatasetAccessLevel = "WRITE";
      expect(component.userHasWriteAccess()).toBe(true);
      component.userDatasetAccessLevel = "READ";
      expect(component.userHasWriteAccess()).toBe(false);
      component.userDatasetAccessLevel = "NONE";
      expect(component.userHasWriteAccess()).toBe(false);
    });
  });

  describe("publicity and downloadable toggles", () => {
    it("marks the dataset public and toasts on success", () => {
      createComponent();
      component.did = 5;
      component.datasetName = "MyDS";
      component.onPublicStatusChange(true);

      expect(component.datasetIsPublic).toBe(true);
      expect(notificationServiceStub.success).toHaveBeenCalledWith("Dataset MyDS is now public");
    });

    it("keeps the public flag and toasts an error when the publicity update fails", () => {
      datasetServiceStub.updateDatasetPublicity.mockReturnValue(throwError(() => new Error("boom")));
      createComponent();
      component.did = 5;
      component.datasetIsPublic = false;
      component.onPublicStatusChange(true);

      expect(component.datasetIsPublic).toBe(false);
      expect(notificationServiceStub.error).toHaveBeenCalledWith("Fail to change the dataset publicity");
    });

    it("marks downloads not-allowed and toasts on success", () => {
      createComponent();
      component.did = 5;
      component.onDownloadableStatusChange(false);

      expect(component.datasetIsDownloadable).toBe(false);
      expect(notificationServiceStub.success).toHaveBeenCalledWith("Dataset downloads are now not allowed");
    });

    it("keeps the downloadable flag and toasts an error when the update fails", () => {
      datasetServiceStub.updateDatasetDownloadable.mockReturnValue(throwError(() => new Error("boom")));
      createComponent();
      component.did = 5;
      component.datasetIsDownloadable = true;
      component.onDownloadableStatusChange(false);

      expect(component.datasetIsDownloadable).toBe(true);
      expect(notificationServiceStub.error).toHaveBeenCalledWith("Failed to change the dataset download permission");
    });

    it("marks the dataset private and names that state, not the one it left", () => {
      createComponent();
      component.did = 5;
      component.datasetName = "MyDS";
      component.datasetIsPublic = true;

      component.onPublicStatusChange(false);

      expect(component.datasetIsPublic).toBe(false);
      expect(notificationServiceStub.success).toHaveBeenCalledWith("Dataset MyDS is now private");
    });

    it("marks downloads allowed and names that state, not the one it left", () => {
      createComponent();
      component.did = 5;
      component.datasetIsDownloadable = false;

      component.onDownloadableStatusChange(true);

      expect(component.datasetIsDownloadable).toBe(true);
      expect(notificationServiceStub.success).toHaveBeenCalledWith("Dataset downloads are now allowed");
    });

    it("does not attempt a publicity change without a dataset id", () => {
      createComponent();
      component.did = undefined;

      component.onPublicStatusChange(true);

      expect(datasetServiceStub.updateDatasetPublicity).not.toHaveBeenCalled();
      expect(component.datasetIsPublic).toBe(false);
    });

    it("does not attempt a download-permission change without a dataset id", () => {
      createComponent();
      component.did = undefined;
      component.datasetIsDownloadable = true;

      component.onDownloadableStatusChange(false);

      expect(datasetServiceStub.updateDatasetDownloadable).not.toHaveBeenCalled();
      expect(component.datasetIsDownloadable).toBe(true);
    });
  });

  describe("creating a version", () => {
    it("hands the panel a call that commits through DatasetService", () => {
      datasetServiceStub.createDatasetVersion.mockReturnValue(of(makeVersion()));
      createComponent();
      component.did = 5;

      component.createDatasetVersion("v2").subscribe();

      expect(datasetServiceStub.createDatasetVersion).toHaveBeenCalledWith(5, "v2");
    });

    it("reloads the version list and the latest-version facts once the panel reports one", () => {
      datasetServiceStub.retrieveDatasetVersionList.mockClear();
      datasetServiceStub.retrieveDatasetLatestVersion.mockClear();
      createComponent();
      component.did = 5;

      component.onVersionCreated();

      expect(datasetServiceStub.retrieveDatasetVersionList).toHaveBeenCalled();
      expect(datasetServiceStub.retrieveDatasetLatestVersion).toHaveBeenCalled();
    });
  });

  describe("downloads", () => {
    it("downloads the selected version as a zip when did and dvid are present", () => {
      createComponent();
      component.did = 5;
      component.datasetName = "DS";
      component.selectedVersion = makeVersion({ dvid: 3, name: "v3" });

      component.onClickDownloadVersionAsZip();

      expect(downloadServiceStub.downloadDatasetVersion).toHaveBeenCalledWith(5, 3, "DS", "v3");
    });

    it("does not download a zip when no version is selected", () => {
      createComponent();
      component.did = 5;
      component.selectedVersion = undefined;

      component.onClickDownloadVersionAsZip();

      expect(downloadServiceStub.downloadDatasetVersion).not.toHaveBeenCalled();
    });

    it("uses the public endpoint to download the current file for a public non-owner dataset", () => {
      createComponent();
      component.did = 5;
      component.selectedVersion = makeVersion({ dvid: 3 });
      component.datasetIsPublic = true;
      component.isOwner = false;
      component.currentDisplayedFileName = "/a/b/c.txt";

      component.onClickDownloadCurrentFile();

      expect(downloadServiceStub.downloadSingleFile).toHaveBeenCalledWith("/a/b/c.txt", false);
    });

    it("uses the authenticated endpoint to download the current file for the owner", () => {
      createComponent();
      component.did = 5;
      component.selectedVersion = makeVersion({ dvid: 3 });
      component.datasetIsPublic = true;
      component.isOwner = true;
      component.currentDisplayedFileName = "/a/b/c.txt";

      component.onClickDownloadCurrentFile();

      expect(downloadServiceStub.downloadSingleFile).toHaveBeenCalledWith("/a/b/c.txt", true);
    });

    it("does not download the current file without a selected version dvid", () => {
      createComponent();
      component.did = 5;
      component.selectedVersion = undefined;

      component.onClickDownloadCurrentFile();

      expect(downloadServiceStub.downloadSingleFile).not.toHaveBeenCalled();
    });
  });

  describe("view flags", () => {
    beforeEach(() => createComponent());

    it("toggles the maximize, right-bar and precise-view-count flags", () => {
      expect(component.isMaximized).toBe(false);
      component.onClickScaleTheView();
      expect(component.isMaximized).toBe(true);

      expect(component.isRightBarCollapsed).toBe(false);
      component.onClickHideRightBar();
      expect(component.isRightBarCollapsed).toBe(true);

      expect(component.displayPreciseViewCount).toBe(false);
      component.changeViewDisplayStyle();
      expect(component.displayPreciseViewCount).toBe(true);
    });
  });

  describe("toggleLike", () => {
    it("unlikes and decrements the like count when currently liked", () => {
      hubServiceStub.postUnlike.mockReturnValue(of(true));
      hubServiceStub.getCounts.mockReturnValue(of([{ counts: { like: 4 } }]));
      createComponent();
      component.did = 5;
      component.currentUid = MOCK_USER.uid;
      component.isLiked = true;
      component.likeCount = 5;

      component.toggleLike();

      expect(hubServiceStub.postUnlike).toHaveBeenCalled();
      expect(component.isLiked).toBe(false);
      expect(component.likeCount).toBe(4);
    });

    it("likes and increments the like count when not currently liked", () => {
      hubServiceStub.postLike.mockReturnValue(of(true));
      hubServiceStub.getCounts.mockReturnValue(of([{ counts: { like: 6 } }]));
      createComponent();
      component.did = 5;
      component.currentUid = MOCK_USER.uid;
      component.isLiked = false;
      component.likeCount = 5;

      component.toggleLike();

      expect(hubServiceStub.postLike).toHaveBeenCalled();
      expect(component.isLiked).toBe(true);
      expect(component.likeCount).toBe(6);
    });

    it("does nothing when no user is logged in", () => {
      createComponent();
      component.did = 5;
      component.currentUid = undefined;

      component.toggleLike();

      expect(hubServiceStub.postLike).not.toHaveBeenCalled();
      expect(hubServiceStub.postUnlike).not.toHaveBeenCalled();
    });

    it("leaves the dataset liked when the server refuses the unlike", () => {
      hubServiceStub.postUnlike.mockReturnValue(of(false));
      // A tally only a refresh could produce, so a refresh that must not happen shows.
      hubServiceStub.getCounts.mockReturnValue(of([{ counts: { like: 99 } }]));
      createComponent();
      component.did = 5;
      component.currentUid = MOCK_USER.uid;
      component.isLiked = true;
      component.likeCount = 5;

      component.toggleLike();

      // The unlike is addressed to this dataset, not to some other entity type
      // that happens to share the id.
      expect(hubServiceStub.postUnlike).toHaveBeenCalledWith(5, EntityType.Dataset);
      // Showing the heart as unfilled after a refused unlike would misreport the
      // stored state, and the next click would then try to like it again.
      expect(component.isLiked).toBe(true);
      expect(component.likeCount).toBe(5);
      expect(hubServiceStub.getCounts).not.toHaveBeenCalled();
    });

    it("leaves the dataset unliked when the server refuses the like", () => {
      hubServiceStub.postLike.mockReturnValue(of(false));
      hubServiceStub.getCounts.mockReturnValue(of([{ counts: { like: 99 } }]));
      createComponent();
      component.did = 5;
      component.currentUid = MOCK_USER.uid;
      component.isLiked = false;
      component.likeCount = 5;

      component.toggleLike();

      expect(hubServiceStub.postLike).toHaveBeenCalledWith(5, EntityType.Dataset);
      expect(component.isLiked).toBe(false);
      expect(component.likeCount).toBe(5);
      expect(hubServiceStub.getCounts).not.toHaveBeenCalled();
    });

    it("reads a refreshed count with no like tally as no likes after unliking", () => {
      hubServiceStub.postUnlike.mockReturnValue(of(true));
      hubServiceStub.getCounts.mockReturnValue(of([{ counts: {} }]));
      createComponent();
      component.did = 5;
      component.currentUid = MOCK_USER.uid;
      component.isLiked = true;
      component.likeCount = 5;

      component.toggleLike();

      // The refresh re-reads this dataset's like tally: a request for another
      // entity, another action or another id would return a stranger's count.
      expect(hubServiceStub.getCounts).toHaveBeenCalledWith([EntityType.Dataset], [5], [ActionType.Like]);
      expect(component.likeCount).toBe(0);
    });

    it("reads a refreshed count with no like tally as no likes after liking", () => {
      hubServiceStub.postLike.mockReturnValue(of(true));
      hubServiceStub.getCounts.mockReturnValue(of([{ counts: {} }]));
      createComponent();
      component.did = 5;
      component.currentUid = MOCK_USER.uid;
      component.isLiked = false;
      component.likeCount = 5;

      component.toggleLike();

      expect(hubServiceStub.getCounts).toHaveBeenCalledWith([EntityType.Dataset], [5], [ActionType.Like]);
      expect(component.likeCount).toBe(0);
    });
  });

  describe("cover image and description persistence", () => {
    it("refreshes the cover url and toasts success after setting a cover image", () => {
      datasetServiceStub.updateDatasetCoverImage.mockReturnValue(of({}));
      datasetServiceStub.getDatasetCoverUrl.mockReturnValue(of({ url: "http://new" }));
      createComponent();
      component.did = 5;
      component.selectedVersion = makeVersion({ name: "v1" });

      component.onSetCoverImage("img.png");

      expect(datasetServiceStub.updateDatasetCoverImage).toHaveBeenCalledWith(5, "v1/img.png");
      expect(component.coverImageUrl).toBe("http://new");
      expect(notificationServiceStub.success).toHaveBeenCalledWith("Cover image updated.");
    });

    it("surfaces the backend message when setting the cover image fails", () => {
      datasetServiceStub.updateDatasetCoverImage.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "nope" }, status: 400 }))
      );
      createComponent();
      component.did = 5;
      component.selectedVersion = makeVersion({ name: "v1" });

      component.onSetCoverImage("img.png");

      expect(notificationServiceStub.error).toHaveBeenCalledWith("nope");
    });

    it("drops the previous cover url when the refreshed one cannot be fetched", () => {
      datasetServiceStub.updateDatasetCoverImage.mockReturnValue(of({}));
      datasetServiceStub.getDatasetCoverUrl.mockReturnValue(throwError(() => new Error("boom")));
      createComponent();
      component.did = 5;
      component.selectedVersion = makeVersion({ name: "v1" });
      component.coverImageUrl = "http://stale";

      component.onSetCoverImage("img.png");

      // The stale url still points at the cover that was just replaced, so keeping
      // it would show the old image as though the change had not been made.
      expect(component.coverImageUrl).toBeNull();
      expect(notificationServiceStub.success).toHaveBeenCalledWith("Cover image updated.");
    });

    it("does not surface a non-HTTP failure's own message when setting the cover image", () => {
      // A rejection from below the HTTP layer, shaped like a response but not one:
      // only a real HttpErrorResponse carries a body the backend meant for a user,
      // so this text must stay out of the toast. Reading `.error.message` off
      // anything that has it would leak the transport detail instead.
      datasetServiceStub.updateDatasetCoverImage.mockReturnValue(
        throwError(() => ({ status: 0, error: { message: "connect ECONNREFUSED 127.0.0.1:8080" } }))
      );
      createComponent();
      component.did = 5;
      component.selectedVersion = makeVersion({ name: "v1" });

      component.onSetCoverImage("img.png");

      expect(notificationServiceStub.error).toHaveBeenCalledWith("Failed to set cover image");
    });

    it("falls back to a generic message when the error body carries none", () => {
      datasetServiceStub.updateDatasetCoverImage.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: {}, status: 500 }))
      );
      createComponent();
      component.did = 5;
      component.selectedVersion = makeVersion({ name: "v1" });

      component.onSetCoverImage("img.png");

      expect(notificationServiceStub.error).toHaveBeenCalledWith("Failed to set cover image");
    });

    it("does nothing when there is no selected version to attach the cover to", () => {
      createComponent();
      component.did = 5;
      component.selectedVersion = undefined;

      component.onSetCoverImage("img.png");

      expect(datasetServiceStub.updateDatasetCoverImage).not.toHaveBeenCalled();
    });

    it("persists a changed description and updates the field", () => {
      datasetServiceStub.updateDatasetDescription.mockReturnValue(of({}));
      createComponent();
      component.did = 5;
      component.datasetDescription = "old";

      component.onDatasetDescriptionChange("new");

      expect(datasetServiceStub.updateDatasetDescription).toHaveBeenCalledWith(5, "new");
      expect(component.datasetDescription).toBe("new");
    });

    it("skips the persistence call when the description is unchanged", () => {
      createComponent();
      component.did = 5;
      component.datasetDescription = "same";

      component.onDatasetDescriptionChange("same");

      expect(datasetServiceStub.updateDatasetDescription).not.toHaveBeenCalled();
    });

    it("reverts the description and toasts an error when persistence fails", () => {
      datasetServiceStub.updateDatasetDescription.mockReturnValue(throwError(() => new Error("boom")));
      createComponent();
      component.did = 5;
      component.datasetDescription = "old";

      component.onDatasetDescriptionChange("new");

      expect(component.datasetDescription).toBe("old");
      expect(notificationServiceStub.error).toHaveBeenCalledWith("Failed to update dataset description");
    });

    it("stores an empty description when the editor hands back nothing", () => {
      // The editor round-trips whatever it was bound to, and a dataset whose stored
      // description is null binds a nullish value straight back out. Persisting that
      // verbatim would write `undefined` over a description instead of clearing it.
      datasetServiceStub.updateDatasetDescription.mockReturnValue(of({}));
      createComponent();
      component.did = 5;
      component.datasetDescription = "old";

      component.onDatasetDescriptionChange(undefined as unknown as string);

      expect(datasetServiceStub.updateDatasetDescription).toHaveBeenCalledWith(5, "");
      expect(component.datasetDescription).toBe("");
    });
  });

  describe("copyCurrentFilePath", () => {
    let originalClipboardDescriptor: PropertyDescriptor | undefined;
    let writeText: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      // Capture the original own-property descriptor (undefined if navigator has no own
      // `clipboard`, e.g. under jsdom) so afterEach can restore the exact shape.
      originalClipboardDescriptor = Object.getOwnPropertyDescriptor(navigator, "clipboard");
      writeText = vi.fn().mockResolvedValue(undefined);
      Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
      createComponent();
    });

    afterEach(() => {
      if (originalClipboardDescriptor) {
        Object.defineProperty(navigator, "clipboard", originalClipboardDescriptor);
      } else {
        delete (navigator as any).clipboard;
      }
    });

    it("writes the displayed path to the clipboard and toasts success", async () => {
      component.currentDisplayedFileName = "/a/b/c.txt";

      await component.copyCurrentFilePath();

      expect(writeText).toHaveBeenCalledWith("/a/b/c.txt");
      expect(notificationServiceStub.success).toHaveBeenCalledWith("File path copied to clipboard");
    });

    it("does nothing when no file is displayed", async () => {
      component.currentDisplayedFileName = "";

      await component.copyCurrentFilePath();

      expect(writeText).not.toHaveBeenCalled();
    });

    it("toasts an error when the clipboard write rejects", async () => {
      writeText.mockRejectedValue(new Error("denied"));
      component.currentDisplayedFileName = "/a/b/c.txt";

      await component.copyCurrentFilePath();

      expect(notificationServiceStub.error).toHaveBeenCalledWith("Failed to copy file path");
    });
  });

  describe("version-node selection", () => {
    it("onVersionFileTreeNodeSelected loads the selected node's content", () => {
      const node = { name: "file.csv", type: "file" } as unknown as Parameters<
        typeof component.onVersionFileTreeNodeSelected
      >[0];
      const loadSpy = vi
        .spyOn(component as unknown as { loadFileContent: (n: unknown) => void }, "loadFileContent")
        .mockImplementation(() => {});

      component.onVersionFileTreeNodeSelected(node);

      expect(loadSpy).toHaveBeenCalledWith(node);
    });
  });

  describe("onPreviouslyUploadedFileDeleted", () => {
    const node: DatasetFileNode = {
      name: "a.txt",
      type: "file",
      parentDir: "/dataset/owner@texera.com/ds/v1/nested",
    };

    it("does not delete a file without a dataset id", () => {
      createComponent();
      component.did = undefined;

      component.onPreviouslyUploadedFileDeleted(node);

      expect(datasetServiceStub.deleteDatasetFile).not.toHaveBeenCalled();
    });

    it("toasts an error and stages nothing when the deletion fails", () => {
      datasetServiceStub.deleteDatasetFile.mockReturnValue(throwError(() => new Error("boom")));
      createComponent();
      component.did = 5;

      component.onPreviouslyUploadedFileDeleted(node);

      expect(notificationServiceStub.error).toHaveBeenCalledWith("Failed to delete the file");
      // A file the backend still holds is not a staged change, so reporting it to the panel
      // would offer a version to create out of a deletion that never happened.
      expect(notificationServiceStub.success).not.toHaveBeenCalled();
    });

    it("deletes by the path relative to the version root, which is what the diff reports", () => {
      createComponent();
      component.did = 5;

      component.onPreviouslyUploadedFileDeleted(node);

      // Only an exact match retires the locally staged entry the panel counts, so any
      // other form of this path would leave the Finished header double-counting.
      expect(datasetServiceStub.deleteDatasetFile).toHaveBeenCalledWith(5, "nested/a.txt");
    });
  });

  describe("onSaveDatasetName", () => {
    it("seeds editedDatasetName from the loaded dataset name", () => {
      datasetServiceStub.getDataset.mockReturnValue(
        of(makeDashboardDataset({ dataset: makeDataset({ name: "seed-name" }) }))
      );
      createComponent();
      component.did = 5;
      component.retrieveDatasetInfo();

      expect(component.editedDatasetName).toBe("seed-name");
    });

    it("persists a valid name unchanged and toasts success", () => {
      datasetServiceStub.updateDatasetName.mockReturnValue(of({}));
      createComponent();
      component.did = 5;
      // Mixed case, hyphen and underscore are all valid: the name must be saved
      // verbatim, not rewritten.
      component.editedDatasetName = "My-Cool_Dataset";

      component.onSaveDatasetName();

      expect(datasetServiceStub.updateDatasetName).toHaveBeenCalledWith(5, "My-Cool_Dataset");
      expect(component.datasetName).toBe("My-Cool_Dataset");
      expect(component.editedDatasetName).toBe("My-Cool_Dataset");
      expect(notificationServiceStub.success).toHaveBeenCalledWith("Dataset name updated to 'My-Cool_Dataset'");
    });

    it("rejects an invalid name with a validation error and does not call the rename API", () => {
      createComponent();
      component.did = 5;
      component.datasetName = "original";
      component.editedDatasetName = "My Cool Dataset"; // spaces are not allowed

      component.onSaveDatasetName();

      expect(datasetServiceStub.updateDatasetName).not.toHaveBeenCalled();
      expect(component.datasetName).toBe("original");
      expect(notificationServiceStub.error).toHaveBeenCalledWith(
        "Invalid dataset name: only letters, numbers, underscores, and hyphens are allowed (max 128 characters)"
      );
    });

    it("toasts an error and leaves the name unchanged when the rename fails", () => {
      datasetServiceStub.updateDatasetName.mockReturnValue(throwError(() => new Error("boom")));
      createComponent();
      component.did = 5;
      component.datasetName = "original";
      component.editedDatasetName = "new-name";

      component.onSaveDatasetName();

      expect(component.datasetName).toBe("original");
      expect(notificationServiceStub.error).toHaveBeenCalledWith("boom");
    });

    it("does nothing when there is no did", () => {
      createComponent();
      component.did = undefined;
      component.editedDatasetName = "whatever";

      component.onSaveDatasetName();

      expect(datasetServiceStub.updateDatasetName).not.toHaveBeenCalled();
    });
  });

  describe("onDeleteDataset", () => {
    it("deletes the dataset, toasts success and navigates back to the dataset list", () => {
      datasetServiceStub.deleteDatasets.mockReturnValue(of({}));
      createComponent();
      const navigateSpy = vi.spyOn(TestBed.inject(Router), "navigate").mockResolvedValue(true);
      component.did = 5;
      component.datasetName = "DS";

      component.onDeleteDataset();

      expect(datasetServiceStub.deleteDatasets).toHaveBeenCalledWith(5);
      expect(notificationServiceStub.success).toHaveBeenCalledWith("Dataset DS was deleted");
      expect(navigateSpy).toHaveBeenCalledWith([USER_DATASET]);
    });

    it("toasts an error and does not navigate when the deletion fails", () => {
      datasetServiceStub.deleteDatasets.mockReturnValue(throwError(() => new Error("boom")));
      createComponent();
      const navigateSpy = vi.spyOn(TestBed.inject(Router), "navigate").mockResolvedValue(true);
      component.did = 5;

      component.onDeleteDataset();

      expect(notificationServiceStub.error).toHaveBeenCalledWith("boom");
      expect(navigateSpy).not.toHaveBeenCalled();
    });

    it("does nothing when there is no did", () => {
      createComponent();
      const navigateSpy = vi.spyOn(TestBed.inject(Router), "navigate").mockResolvedValue(true);
      component.did = undefined;

      component.onDeleteDataset();

      expect(datasetServiceStub.deleteDatasets).not.toHaveBeenCalled();
      expect(navigateSpy).not.toHaveBeenCalled();
    });
  });

  describe("delete button disabled state", () => {
    // The Settings tab (and its Delete card) only render for WRITE access; the
    // delete button itself is owner-only, mirroring the Downloadable switch's
    // [nzDisabled]="!isOwner". Renders WRITE access with the given ownership,
    // activates the (inactive) Settings tab so its pane is in the DOM, then
    // returns the delete button element.
    const renderDeleteButton = (isOwner: boolean): HTMLButtonElement => {
      datasetServiceStub.getDataset.mockReturnValue(of(makeDashboardDataset({ accessPrivilege: "WRITE", isOwner })));
      createComponent();
      fixture.detectChanges();

      const tabButtons: NodeListOf<HTMLElement> = fixture.nativeElement.querySelectorAll(".ant-tabs-tab-btn");
      const settingsTab = Array.from(tabButtons).find(tab => tab.textContent?.includes("Settings"));
      expect(settingsTab).toBeTruthy();
      (settingsTab as HTMLElement).click();
      fixture.detectChanges();

      return fixture.nativeElement.querySelector('button[title="Delete"]') as HTMLButtonElement;
    };

    it("disables the delete button for a non-owner with write access", () => {
      const button = renderDeleteButton(false);

      expect(button).toBeTruthy();
      expect(button.disabled).toBe(true);
    });

    it("enables the delete button for the owner", () => {
      const button = renderDeleteButton(true);

      expect(button).toBeTruthy();
      expect(button.disabled).toBe(false);
    });
  });

  describe("contributors", () => {
    const contributorA: Contributor = {
      name: "Contributor A",
      creator: true,
      affiliation: "Test Lab",
      email: "contributor-a@test.com",
      comments: "",
    };
    const contributorB: Contributor = {
      name: "Contributor B",
      creator: false,
      affiliation: "Test Lab",
      email: "contributor-b@test.com",
      comments: "notes",
    };

    it("maps contributors from the dashboard dataset and falls back to an empty list", () => {
      datasetServiceStub.getDataset.mockReturnValue(of(makeDashboardDataset({ contributors: [contributorA] })));
      createComponent();
      component.did = 5;

      component.retrieveDatasetInfo();
      expect(component.datasetContributors).toEqual([contributorA]);

      datasetServiceStub.getDataset.mockReturnValue(of(makeDashboardDataset()));
      component.retrieveDatasetInfo();
      expect(component.datasetContributors).toEqual([]);
    });

    it("onAddContributor appends the modal result and persists the list", () => {
      modalServiceStub.create.mockReturnValue({ afterClose: of(contributorB) });
      createComponent();
      component.did = 5;
      component.datasetContributors = [contributorA];

      component.onAddContributor();

      expect(component.datasetContributors).toEqual([contributorA, contributorB]);
      expect(datasetServiceStub.updateDatasetContributors).toHaveBeenCalledWith(5, [contributorA, contributorB]);
      expect(notificationServiceStub.success).toHaveBeenCalledWith("Contributors updated");
    });

    it("onAddContributor does not persist when the modal is cancelled", () => {
      modalServiceStub.create.mockReturnValue({ afterClose: of(undefined) });
      createComponent();
      component.did = 5;
      component.datasetContributors = [contributorA];

      component.onAddContributor();

      expect(component.datasetContributors).toEqual([contributorA]);
      expect(datasetServiceStub.updateDatasetContributors).not.toHaveBeenCalled();
    });

    it("onEditContributor replaces the edited row and persists the list", () => {
      const updated = { ...contributorA, affiliation: "Another Test Lab" };
      modalServiceStub.create.mockReturnValue({ afterClose: of(updated) });
      createComponent();
      component.did = 5;
      component.datasetContributors = [contributorA, contributorB];

      component.onEditContributor(contributorA);

      expect(component.datasetContributors).toEqual([updated, contributorB]);
      expect(datasetServiceStub.updateDatasetContributors).toHaveBeenCalledWith(5, [updated, contributorB]);
    });

    it("onDeleteContributor removes the row and persists the list", () => {
      createComponent();
      component.did = 5;
      component.datasetContributors = [contributorA, contributorB];

      component.onDeleteContributor(contributorA);

      expect(component.datasetContributors).toEqual([contributorB]);
      expect(datasetServiceStub.updateDatasetContributors).toHaveBeenCalledWith(5, [contributorB]);
    });

    it("rolls the list back and notifies when persisting fails", () => {
      datasetServiceStub.updateDatasetContributors.mockReturnValue(throwError(() => new Error("boom")));
      createComponent();
      component.did = 5;
      component.datasetContributors = [contributorA, contributorB];

      component.onDeleteContributor(contributorB);

      expect(component.datasetContributors).toEqual([contributorA, contributorB]);
      expect(notificationServiceStub.error).toHaveBeenCalledWith("Failed to update contributors");
    });

    it("does not call the service when did is missing", () => {
      createComponent();
      component.did = undefined;
      component.datasetContributors = [contributorA];

      component.onDeleteContributor(contributorA);

      expect(datasetServiceStub.updateDatasetContributors).not.toHaveBeenCalled();
    });
  });

  // ─── template rendering ────────────────────────────────────────────────────
  // These drive the markup through the DOM (rather than calling handlers directly)
  // so the template's bindings and conditional blocks actually execute.
  describe("template rendering", () => {
    // Renders the component and applies the given state, so each *ngIf arm is exercised.
    // The first detectChanges() lets ngOnInit's subscriptions settle — they reset fields
    // such as coverImageUrl — so the state is applied afterwards and rendered by a
    // second change-detection pass.
    const renderWith = (state: Partial<DatasetDetailComponent> = {}): void => {
      createComponent();
      fixture.detectChanges();
      Object.assign(component, state);
      fixture.detectChanges();
    };

    const clickByCss = (selector: string): void => {
      const el = fixture.debugElement.query(By.css(selector));
      expect(el).toBeTruthy();
      el.triggerEventHandler("click", null);
      fixture.detectChanges();
    };

    // nz-tabs renders only the active tab's content, so a tab must be opened by its
    // title before the markup inside it can be queried.
    const openTab = (title: string): void => {
      const tab = fixture.debugElement
        .queryAll(By.css(".ant-tabs-tab"))
        .find(el => (el.nativeElement.textContent ?? "").includes(title));
      expect(tab).toBeTruthy();
      tab!.nativeElement.click();
      fixture.detectChanges();
    };

    it("toggles the like through the like tag when logged in", () => {
      // toggleLike() early-returns unless currentUid is set, which login() supplies
      createComponent();
      fixture.detectChanges();
      login();
      Object.assign(component, { isLogin: true, did: 5, isLiked: false, likeCount: 1 });
      fixture.detectChanges();

      clickByCss(".like-tag");

      expect(hubServiceStub.postLike).toHaveBeenCalled();
    });

    it("unlikes through the same tag when the dataset is already liked", () => {
      createComponent();
      fixture.detectChanges();
      login();
      Object.assign(component, { isLogin: true, did: 5, isLiked: true, likeCount: 2 });
      fixture.detectChanges();

      clickByCss(".like-tag");

      expect(hubServiceStub.postUnlike).toHaveBeenCalled();
    });

    it("does not toggle the like when logged out", () => {
      renderWith({ isLogin: false, did: 5, isLiked: false, likeCount: 1 });

      const likeTag = fixture.debugElement.query(By.css(".like-tag"));
      expect(likeTag).toBeTruthy();
      // the template guards the handler with `isLogin &&`
      expect(likeTag.nativeElement.classList).toContain("disabled");

      likeTag.triggerEventHandler("click", null);

      expect(hubServiceStub.postLike).not.toHaveBeenCalled();
    });

    it("omits the cover image when there is no cover URL", () => {
      renderWith({ coverImageUrl: null });
      expect(fixture.debugElement.query(By.css(".dataset-cover-image"))).toBeNull();
    });

    it("renders the cover image bound to the cover URL", () => {
      renderWith({ coverImageUrl: "blob:cover" });
      const img = fixture.debugElement.query(By.css(".dataset-cover-image"));
      expect(img).toBeTruthy();
      expect(img.nativeElement.getAttribute("src")).toBe("blob:cover");
    });

    it("collapses the right bar from the template, then renders the restore control", () => {
      renderWith({ isRightBarCollapsed: false });
      openTab("Versions & Files");

      // both arms of the *ngIf pair are exercised: hide first, then the show button
      clickByCss("button[nz-tooltip='Hide the right bar']");
      expect(component.isRightBarCollapsed).toBe(true);

      clickByCss("button[nz-tooltip='Show Tree']");
      expect(component.isRightBarCollapsed).toBe(false);
    });

    it("binds the dataset name input and saves it from the template", () => {
      // the Settings tab is behind *ngIf="userHasWriteAccess()"
      renderWith({ did: 5, editedDatasetName: "renamed", userDatasetAccessLevel: "WRITE" });
      openTab("Settings");

      const input = fixture.debugElement.query(By.css(".settings-name-controls input[nz-input]"));
      expect(input).toBeTruthy();

      // drive the [(ngModel)] update path through the DOM
      input.nativeElement.value = "typed-name";
      input.nativeElement.dispatchEvent(new Event("input"));
      fixture.detectChanges();
      expect(component.editedDatasetName).toBe("typed-name");

      const saveBtn = fixture.debugElement
        .queryAll(By.css("button"))
        .find(btn => (btn.nativeElement.textContent ?? "").trim() === "Save");
      expect(saveBtn).toBeTruthy();
      saveBtn!.triggerEventHandler("click", null);

      expect(datasetServiceStub.updateDatasetName).toHaveBeenCalledWith(5, "typed-name");
    });

    it("renders every contributor row from the list", () => {
      renderWith({
        did: 5,
        datasetContributors: [
          { name: "Ada", email: "ada@x.io", affiliation: "" } as Contributor,
          { name: "Grace", email: "grace@x.io", affiliation: "" } as Contributor,
        ],
      });

      const rendered = fixture.debugElement.nativeElement.textContent ?? "";
      expect(rendered).toContain("Ada");
      expect(rendered).toContain("Grace");
    });

    it("routes the settings switches' ngModelChange bindings to the service", () => {
      renderWith({
        did: 5,
        datasetIsPublic: false,
        datasetIsDownloadable: true,
        userDatasetAccessLevel: "WRITE",
        isOwner: true, // the downloadable switch is [nzDisabled]="!isOwner"
      });
      openTab("Settings");

      const switches = fixture.debugElement.queryAll(By.css("nz-switch"));
      expect(switches.length).toBeGreaterThanOrEqual(2);

      // fire the template's (ngModelChange) handlers rather than calling the methods
      switches[0].triggerEventHandler("ngModelChange", true);
      expect(datasetServiceStub.updateDatasetPublicity).toHaveBeenCalledWith(5);

      switches[1].triggerEventHandler("ngModelChange", false);
      expect(datasetServiceStub.updateDatasetDownloadable).toHaveBeenCalledWith(5);
    });

    // ─── contributor management ─────────────────────────────────────────────
    const contributors = [
      { name: "Ada", email: "ada@x.io", affiliation: "" } as Contributor,
      { name: "Grace", email: "grace@x.io", affiliation: "" } as Contributor,
    ];

    it("renders a row per contributor with the actions trigger", () => {
      renderWith({ did: 5, datasetContributors: [...contributors], userDatasetAccessLevel: "WRITE" });

      const rendered = fixture.nativeElement.textContent ?? "";
      expect(rendered).toContain("Ada");
      expect(rendered).toContain("Grace");
      // each row carries the dropdown trigger that hosts Edit/Delete
      const triggers = fixture.debugElement
        .queryAll(By.css("button[nz-dropdown]"))
        .filter(btn => btn.nativeElement.querySelector("i.anticon-more"));
      expect(triggers.length).toBe(contributors.length);
    });

    // Edit/Delete live inside an nz-dropdown-menu, which only mounts into a CDK overlay on a
    // real user open — jsdom does not drive that. Assert the handlers those menu items bind to
    // instead; the rendered trigger is covered above.
    it("edits the chosen contributor through the menu's binding target", () => {
      const updated = { ...contributors[0], affiliation: "Lab" };
      modalServiceStub.create.mockReturnValue({ afterClose: of(updated) });
      renderWith({ did: 5, datasetContributors: [...contributors], userDatasetAccessLevel: "WRITE" });

      component.onEditContributor(contributors[0]);

      expect(component.datasetContributors[0]).toEqual(updated);
    });

    it("deletes the chosen contributor through the popconfirm's binding target", () => {
      renderWith({ did: 5, datasetContributors: [...contributors], userDatasetAccessLevel: "WRITE" });

      component.onDeleteContributor(contributors[0]);

      expect(component.datasetContributors.map(c => c.name)).toEqual(["Grace"]);
    });

    // ─── view controls ──────────────────────────────────────────────────────

    it("downloads the current file from the toolbar", () => {
      // the toolbar controls are behind *ngIf="selectedVersion"
      renderWith({ did: 5, selectedVersion: { dvid: 1, name: "v1" } as DatasetVersion });
      openTab("Versions & Files");
      const onDownload = vi.spyOn(component, "onClickDownloadCurrentFile").mockImplementation(() => {});

      const downloadBtn = fixture.debugElement
        .queryAll(By.css("button"))
        .find(btn => btn.nativeElement.querySelector("i.anticon-download"));
      expect(downloadBtn).toBeTruthy();
      downloadBtn!.triggerEventHandler("click", null);

      expect(onDownload).toHaveBeenCalled();
    });

    it("toggles the scaled view from the toolbar", () => {
      renderWith({ did: 5, isMaximized: false, selectedVersion: { dvid: 1, name: "v1" } as DatasetVersion });
      openTab("Versions & Files");

      const scaleBtn = fixture.debugElement
        .queryAll(By.css("button"))
        .find(btn => btn.nativeElement.querySelector("i.anticon-expand, i.anticon-compress"));
      expect(scaleBtn).toBeTruthy();
      scaleBtn!.triggerEventHandler("click", null);
      fixture.detectChanges();

      expect(component.isMaximized).toBe(true);
    });

    // ─── sider resize ───────────────────────────────────────────────────────

    it("applies the dragged sider width on the next animation frame", async () => {
      renderWith({ did: 5 });

      component.onSideResize({ width: 321 } as NzResizeEvent);
      // the handler defers to requestAnimationFrame; let that frame run
      await new Promise(resolve => requestAnimationFrame(() => resolve(null)));

      expect(component.siderWidth).toBe(321);
    });

    it("cancels the frame the previous resize scheduled", () => {
      renderWith({ did: 5 });
      // Hand out a known frame id so the assertion below pins down *which* frame is
      // cancelled: the component starts with id = -1, so merely asserting that
      // cancelAnimationFrame was called would pass even if the id were never tracked.
      const request = vi.spyOn(globalThis, "requestAnimationFrame").mockReturnValue(100);
      const cancel = vi.spyOn(globalThis, "cancelAnimationFrame");
      try {
        component.onSideResize({ width: 100 } as NzResizeEvent);
        cancel.mockClear(); // drop the initial cancel(-1)

        component.onSideResize({ width: 200 } as NzResizeEvent);

        expect(cancel).toHaveBeenCalledWith(100);
      } finally {
        cancel.mockRestore();
        request.mockRestore();
      }
    });
  });
});

/**
 * The explorer's markup carries a lot of behaviour that never shows up in the
 * component's own API: which icon labels a status tag, which contributor a row
 * menu acts on, whether a toolbar button reaches the download service at all.
 * Everything below drives the real template — real children, real overlays — and
 * asserts on what is rendered, so a binding that quietly changes meaning fails.
 */
describe("DatasetDetailComponent rendered template", () => {
  let fixture: ComponentFixture<DatasetDetailComponent>;
  let component: DatasetDetailComponent;

  type Stub = Record<string, ReturnType<typeof vi.fn>>;
  let datasetService: Stub;
  let downloadService: Stub;
  let notificationService: Stub;
  let modalService: Stub;
  let hubService: Stub;

  const OWNER = "owner@texera.com";

  const aVersion = (over: Partial<DatasetVersion> = {}): DatasetVersion =>
    ({ dvid: 11, did: 5, creatorUid: 9, name: "v1", ...over }) as DatasetVersion;

  const makeFileItem = (name: string): FileUploadItem => ({
    file: new File(["x"], name),
    name,
    description: "",
    uploadProgress: 0,
    isUploadingFlag: false,
    restart: false,
  });

  beforeEach(() => {
    TestBed.resetTestingModule();

    datasetService = {
      getDataset: vi.fn(() =>
        of({
          isOwner: true,
          ownerEmail: OWNER,
          accessPrivilege: "WRITE",
          size: 0,
          dataset: {
            did: 5,
            ownerUid: 9,
            name: "ds",
            isPublic: false,
            isDownloadable: true,
            description: "desc",
          },
        })
      ),
      retrieveDatasetVersionList: vi.fn(() => of([])),
      retrieveDatasetLatestVersion: vi.fn(() => of(aVersion())),
      retrieveDatasetVersionFileTree: vi.fn(() => of({ fileNodes: [], size: 1024 })),
      // The real file renderer is rendered here, and it fetches whatever file is on screen.
      retrieveDatasetVersionSingleFile: vi.fn(() => of(new Blob(["a,b"], { type: "text/csv" }))),
      getDatasetCoverUrl: vi.fn(() => of({ url: "http://cover" })),
      getDatasetDiff: vi.fn(() => of([])),
      createDatasetVersion: vi.fn(() => of(aVersion())),
      updateDatasetPublicity: vi.fn(() => of({})),
      updateDatasetDownloadable: vi.fn(() => of({})),
      updateDatasetCoverImage: vi.fn(() => of({})),
      updateDatasetDescription: vi.fn(() => of({})),
      updateDatasetContributors: vi.fn(() => of(undefined)),
      updateDatasetName: vi.fn(() => of({})),
      deleteDatasets: vi.fn(() => of({})),
      deleteDatasetFile: vi.fn(() => of({})),
      // Never completes, so an upload started from the template stays in flight
      // and its row keeps rendering the "uploading" arm.
      multipartUpload: vi.fn(() => new Subject<MultipartUploadProgress>().asObservable()),
      finalizeMultipartUpload: vi.fn(() => of({})),
    };
    downloadService = {
      downloadDatasetVersion: vi.fn(() => of(new Blob())),
      downloadSingleFile: vi.fn(() => of(new Blob())),
    };
    notificationService = { success: vi.fn(), error: vi.fn(), info: vi.fn() };
    modalService = { create: vi.fn(() => ({ afterClose: of(undefined) })) };
    hubService = {
      getCounts: vi.fn(() => of([{ counts: { like: 0 } }])),
      postView: vi.fn(() => of(0)),
      isLiked: vi.fn(() => of([{ isLiked: false }])),
      postLike: vi.fn(() => of(true)),
      postUnlike: vi.fn(() => of(true)),
    };

    TestBed.configureTestingModule({
      imports: [DatasetDetailComponent, NoopAnimationsModule, ...commonTestImports],
      providers: [
        { provide: ActivatedRoute, useValue: { params: of({ did: 5 }), data: of({}) } },
        { provide: NzModalService, useValue: modalService },
        { provide: DatasetService, useValue: datasetService },
        { provide: NotificationService, useValue: notificationService },
        { provide: DownloadService, useValue: downloadService },
        { provide: UserService, useClass: StubUserService },
        { provide: HubService, useValue: hubService },
        { provide: AdminSettingsService, useValue: { getPublicSetting: vi.fn(() => of("3")) } },
        { provide: MarkdownService, useValue: { parse: vi.fn(() => "") } },
        ...commonTestProviders,
      ],
    });

    fixture = TestBed.createComponent(DatasetDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    fixture?.destroy();
    document.querySelectorAll(".cdk-overlay-container").forEach(container => (container.innerHTML = ""));
  });

  /** Applies state on top of what ngOnInit produced and renders it. */
  const render = (state: Partial<DatasetDetailComponent> = {}): HTMLElement => {
    Object.assign(component, state);
    fixture.detectChanges();
    return fixture.nativeElement as HTMLElement;
  };

  /** Asserts the element exists, so a stale selector fails as "not found". */
  const q = <E extends Element>(root: ParentNode, selector: string): E => {
    const el = root.querySelector(selector);
    expect(el, `expected to find "${selector}"`).not.toBeNull();
    return el as unknown as E;
  };

  /** Renders the fixture and the CDK overlays hanging off it. */
  const flush = (): void => {
    fixture.detectChanges();
    TestBed.inject(ApplicationRef).tick();
  };

  const overlay = (): HTMLElement => q<HTMLElement>(document, ".cdk-overlay-container");

  // nz-dropdown audits its own visibility stream for 150ms before it opens an overlay.
  const settleOverlay = async (): Promise<void> => {
    await new Promise(resolve => setTimeout(resolve, 200));
    flush();
  };

  const text = (el: Element | null | undefined): string => (el?.textContent ?? "").replace(/\s+/g, " ").trim();

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

  /** Expands the collapse panel whose header contains the given text. */
  const openPanel = (header: string): void => {
    const found = Array.from(
      (fixture.nativeElement as HTMLElement).querySelectorAll<HTMLElement>(".ant-collapse-header")
    ).find(h => (h.textContent ?? "").includes(header));
    expect(found, `expected a collapse panel headed "${header}"`).toBeDefined();
    found!.click();
    fixture.detectChanges();
  };

  /** The toolbar button carrying the given nz-tooltip title. */
  const byTooltip = (title: string): HTMLButtonElement | undefined =>
    Array.from((fixture.nativeElement as HTMLElement).querySelectorAll<HTMLButtonElement>("button")).find(
      b => b.getAttribute("nz-tooltip") === title
    );

  /** The nz-icon names rendered inside an element, read from their aria-labels. */
  const iconNames = (root: ParentNode): string[] =>
    Array.from(root.querySelectorAll<HTMLElement>(".anticon")).map(i => i.getAttribute("aria-label") ?? "");

  describe("status tags", () => {
    const tags = (): HTMLElement[] =>
      Array.from((fixture.nativeElement as HTMLElement).querySelectorAll<HTMLElement>(".status-tag"));

    it("labels a public, downloadable dataset with the globe and download icons", () => {
      render({ datasetIsPublic: true, datasetIsDownloadable: true });

      const [visibility, downloadable] = tags();
      expect(text(visibility)).toBe("Public");
      expect(iconNames(visibility)).toEqual(["global"]);
      expect(visibility.classList).toContain("tag-public");

      expect(text(downloadable)).toBe("Downloadable");
      expect(iconNames(downloadable)).toEqual(["download"]);
      expect(downloadable.classList).toContain("tag-downloadable");
    });

    it("labels a private, download-restricted dataset with the lock and stop icons", () => {
      // The other leg of each tag: a visitor must be able to tell at a glance
      // that the dataset is neither public nor downloadable.
      render({ datasetIsPublic: false, datasetIsDownloadable: false });

      const [visibility, downloadable] = tags();
      expect(text(visibility)).toBe("Private");
      expect(iconNames(visibility)).toEqual(["lock"]);
      expect(visibility.classList).not.toContain("tag-public");

      expect(text(downloadable)).toBe("Download restricted");
      expect(iconNames(downloadable)).toEqual(["stop"]);
      expect(downloadable.classList).not.toContain("tag-downloadable");
    });

    it("tells the view counter and the like counter apart", () => {
      // The two counters are adjacent tags that differ only in which field they
      // read, so each needs a count the other cannot produce.
      render({ viewCount: 1500, likeCount: 3 });

      const [, , views, likes] = tags();
      expect(iconNames(views)).toEqual(["eye"]);
      // Counts are abbreviated once they reach a thousand, not printed raw.
      expect(text(views)).toBe("1.5k");

      expect(iconNames(likes)).toEqual(["like"]);
      expect(likes.classList).toContain("like-tag");
      expect(text(likes)).toBe("3");
    });
  });

  describe("settings hints", () => {
    // Visibility and Downloadable are near-identical rows, so a hint or a switch
    // is only meaningful next to the label it belongs to: reading them as one
    // unordered pile would pass just as happily with the two rows exchanged.
    const settingsRow = (el: HTMLElement, label: string): HTMLElement => {
      const row = Array.from(el.querySelectorAll<HTMLElement>(".settings-name-row")).find(
        r => text(r.querySelector("label")) === label
      );
      expect(row, `expected a settings row labelled "${label}"`).toBeDefined();
      return row!;
    };

    const hintOf = (el: HTMLElement, label: string): string =>
      text(q<HTMLElement>(settingsRow(el, label), ".settings-hint"));

    const switchIsOn = (el: HTMLElement, label: string): boolean =>
      q<HTMLElement>(settingsRow(el, label), "nz-switch button").classList.contains("ant-switch-checked");

    it("spells out what public visibility and blocked downloads mean", () => {
      render({ userDatasetAccessLevel: "WRITE", datasetIsPublic: true, datasetIsDownloadable: false });
      const el = openTab("Settings");

      expect(hintOf(el, "Visibility")).toBe("Public — anyone can view this dataset.");
      expect(hintOf(el, "Downloadable")).toBe("Viewers can browse files but cannot download them.");
      // The switch beside each hint has to report the same state the prose does.
      expect(switchIsOn(el, "Visibility")).toBe(true);
      expect(switchIsOn(el, "Downloadable")).toBe(false);
    });

    it("spells out what private visibility and permitted downloads mean", () => {
      render({ userDatasetAccessLevel: "WRITE", datasetIsPublic: false, datasetIsDownloadable: true });
      const el = openTab("Settings");

      expect(hintOf(el, "Visibility")).toBe("Private — only you and invited collaborators can see this dataset.");
      expect(hintOf(el, "Downloadable")).toBe("Viewers can download this dataset.");
      expect(switchIsOn(el, "Visibility")).toBe(false);
      expect(switchIsOn(el, "Downloadable")).toBe(true);
    });
  });

  describe("contributor row menu", () => {
    const ada: Contributor = { name: "Ada", email: "ada@x.io", affiliation: "Lab A", comments: "", creator: true };
    const grace: Contributor = {
      name: "Grace",
      email: "grace@x.io",
      affiliation: "Lab B",
      comments: "",
      creator: false,
    };

    beforeEach(() => render({ did: 5, datasetContributors: [ada, grace], userDatasetAccessLevel: "WRITE" }));

    /** Opens the actions dropdown on the card at `index` and returns its menu. */
    const openRowMenu = async (index: number): Promise<HTMLElement> => {
      const cards = (fixture.nativeElement as HTMLElement).querySelectorAll<HTMLElement>(".contributor-card");
      expect(cards.length).toBeGreaterThan(index);
      q<HTMLButtonElement>(cards[index], ".contributor-actions").click();
      await settleOverlay();
      // Each card declares its own menu template, so exactly one may be open.
      const menus = overlay().querySelectorAll<HTMLElement>(".contributor-actions-menu");
      expect(menus.length).toBe(1);
      return menus[0];
    };

    const menuItem = (menu: HTMLElement, label: string): HTMLElement => {
      const item = Array.from(menu.querySelectorAll<HTMLElement>("li")).find(li => text(li) === label);
      expect(item, `expected a menu item labelled "${label}"`).toBeDefined();
      return item!;
    };

    it("edits the contributor whose own row menu was used", async () => {
      // The menu is declared inside the *ngFor, so its handlers have to close over
      // that row's contributor rather than the first one in the list.
      menuItem(await openRowMenu(1), "Edit").click();
      flush();

      expect(modalService.create).toHaveBeenCalledWith(
        expect.objectContaining({ nzTitle: "Edit Contributor", nzData: grace })
      );
    });

    it("deletes the contributor whose own row menu was used, once the deletion is confirmed", async () => {
      menuItem(await openRowMenu(1), "Delete").click();
      flush();

      // The first click only asks; the row survives until the confirmation is accepted.
      expect(text(q<HTMLElement>(overlay(), ".ant-popover-inner"))).toContain('Delete contributor "Grace"?');
      expect(datasetService.updateDatasetContributors).not.toHaveBeenCalled();

      const confirm = Array.from(overlay().querySelectorAll<HTMLButtonElement>(".ant-popover-buttons button")).find(
        b => text(b) === "Delete"
      );
      expect(confirm, "expected a Delete button in the confirmation").toBeDefined();
      confirm!.click();
      flush();

      expect(datasetService.updateDatasetContributors).toHaveBeenCalledWith(5, [ada]);
    });

    it("adds a contributor from the keyboard on the add tile", () => {
      const tile = q<HTMLElement>(fixture.nativeElement, ".contributor-card-add");

      tile.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
      flush();
      expect(modalService.create).toHaveBeenCalledTimes(1);
      expect(modalService.create).toHaveBeenLastCalledWith(expect.objectContaining({ nzTitle: "Add Contributor" }));

      const space = new KeyboardEvent("keydown", { key: " ", bubbles: true, cancelable: true });
      tile.dispatchEvent(space);
      flush();
      // Space activates the tile instead of scrolling the panel.
      expect(space.defaultPrevented).toBe(true);
      expect(modalService.create).toHaveBeenCalledTimes(2);
    });
  });

  describe("file toolbar", () => {
    beforeEach(() => {
      render({ did: 5, selectedVersion: aVersion(), currentDisplayedFileName: "v1/a.csv", isLogin: true });
      openTab("Versions & Files");
    });

    it("downloads the file that is on screen", () => {
      // "On screen" has to mean the file the renderer beside the button fetched,
      // not merely the field the test happened to set.
      expect(datasetService.retrieveDatasetVersionSingleFile).toHaveBeenCalledWith("v1/a.csv", true);
      expect(fixture.debugElement.query(By.css("texera-user-dataset-file-renderer")).componentInstance.filePath).toBe(
        "v1/a.csv"
      );

      byTooltip("Download the file")!.click();

      expect(downloadService.downloadSingleFile).toHaveBeenCalledWith("v1/a.csv", true);
    });

    it("downloads the file that is on screen over the public endpoint for a non-owner", () => {
      // The authenticated endpoint is the wrong one here: a visitor to somebody
      // else's public dataset has no private access to fall back on.
      render({ datasetIsPublic: true, datasetIsDownloadable: true, isOwner: false, userDatasetAccessLevel: "READ" });

      const button = byTooltip("Download the file")!;
      expect(button.disabled).toBe(false);
      button.click();

      expect(downloadService.downloadSingleFile).toHaveBeenCalledWith("v1/a.csv", false);
    });

    it("keeps the owner of a public dataset on the authenticated endpoint", () => {
      // Publicity alone does not decide the endpoint: the owner still has private
      // access, and the public route would hide their own unpublished changes.
      render({ datasetIsPublic: true, isOwner: true });

      byTooltip("Download the file")!.click();

      expect(downloadService.downloadSingleFile).toHaveBeenCalledWith("v1/a.csv", true);
    });

    it("maximizes the view from the toolbar and offers the way back", () => {
      const el = fixture.nativeElement as HTMLElement;
      expect(el.querySelector(".dataset-header")).not.toBeNull();

      byTooltip("Maximize View")!.click();
      fixture.detectChanges();

      // Maximizing drops the dataset header so the file fills the pane.
      expect(el.querySelector(".dataset-header")).toBeNull();
      expect(byTooltip("Maximize View")).toBeUndefined();

      byTooltip("Minimize View")!.click();
      fixture.detectChanges();

      expect(el.querySelector(".dataset-header")).not.toBeNull();
      expect(byTooltip("Minimize View")).toBeUndefined();
    });

    it("applies a width the resize handle reports, between the bounds it declares", async () => {
      const sider = fixture.debugElement.query(By.css("nz-sider"));
      expect(sider.nativeElement.style.width).toBe("400px");

      // The drag itself belongs to NzResizableDirective; what this component owns
      // is the bounds it hands the directive and what it does with the reported
      // width. Both have to be pinned, and in the right order — swapped bounds
      // would let the handle collapse the sider past its minimum.
      const resizable = sider.injector.get(NzResizableDirective);
      expect(resizable.nzMinWidth).toBe(component.MIN_SIDER_WIDTH);
      expect(resizable.nzMaxWidth).toBe(component.MAX_SIDER_WIDTH);
      expect(resizable.nzMinWidth).toBeLessThan(resizable.nzMaxWidth as number);

      sider.triggerEventHandler("nzResize", { width: 520 });
      // The new width is applied on the next animation frame.
      await new Promise(resolve => requestAnimationFrame(() => resolve(null)));
      fixture.detectChanges();

      expect(sider.nativeElement.style.width).toBe("520px");
    });
  });

  describe("version picker", () => {
    const v1 = aVersion({ dvid: 11, name: "v1" });
    const v2 = aVersion({ dvid: 12, name: "v2" });
    const v3 = aVersion({ dvid: 13, name: "v3" });

    beforeEach(() => {
      render({ did: 5, datasetName: "ds", versions: [v1, v2, v3], selectedVersion: v1, isLogin: true });
      openTab("Versions & Files");
    });

    it("offers every known version and loads the one that is picked", async () => {
      const select = fixture.debugElement.query(By.css("nz-select"));
      /** Picks a version through the control and reports the name it then shows. */
      const pick = async (version: DatasetVersion): Promise<string> => {
        select.triggerEventHandler("ngModelChange", version);
        fixture.detectChanges();
        // ngModel pushes the new value into the control in a microtask.
        await Promise.resolve();
        fixture.detectChanges();
        return text(q<HTMLElement>(fixture.nativeElement, ".ant-select-selection-item"));
      };
      expect(text(q<HTMLElement>(fixture.nativeElement, ".ant-select-selection-item"))).toBe("v1");

      // The picker fans out over the whole list: every version has to be offered under
      // its own name, not only the first one, which the control already shows.
      expect([await pick(v2), await pick(v3), await pick(v1)]).toEqual(["v2", "v3", "v1"]);

      // The third argument decides whether the tree is fetched over the
      // authenticated or the anonymous endpoint, so it has to be the real flag.
      expect(datasetService.retrieveDatasetVersionFileTree).toHaveBeenCalledWith(5, 12, true);
      expect(datasetService.retrieveDatasetVersionFileTree).toHaveBeenCalledWith(5, 13, true);
    });

    it("loads a picked version over the anonymous endpoint when nobody is signed in", () => {
      render({ isLogin: false });

      fixture.debugElement.query(By.css("nz-select")).triggerEventHandler("ngModelChange", v2);

      expect(datasetService.retrieveDatasetVersionFileTree).toHaveBeenCalledWith(5, 12, false);
    });

    it("downloads the whole selected version as a zip", () => {
      byTooltip("Download Dataset")!.click();

      expect(downloadService.downloadDatasetVersion).toHaveBeenCalledWith(5, 11, "ds", "v1");
    });
  });

  describe("version file tree", () => {
    const tree = (): DebugElement => fixture.debugElement.query(By.css("texera-user-dataset-version-filetree"));
    // The first four segments (datasets/owner/dataset/version) are the prefix the
    // relative path strips, so "nested" is the first segment the backend sees.
    const leaf = (name: string): DatasetFileNode => ({
      name,
      type: "file",
      parentDir: `/dataset/${OWNER}/ds/v1/nested`,
      size: 2048,
    });

    beforeEach(() => {
      render({ did: 5, selectedVersion: aVersion({ name: "v1" }) });
      openTab("Versions & Files");
    });

    it("hands the tree the nodes of the version on screen", () => {
      const nodes = [leaf("b.csv"), leaf("c.csv")];
      render({ fileTreeNodeList: nodes });

      expect(tree().componentInstance.fileTreeNodes).toEqual(nodes);
    });

    it("shows the file the tree selected", () => {
      expect(text(q<HTMLElement>(fixture.nativeElement, ".file-title-main"))).not.toContain("b.csv");

      tree().triggerEventHandler("selectedTreeNode", leaf("b.csv"));
      fixture.detectChanges();

      // The heading is the full path — the copy-path button beside it copies
      // exactly this string — not the bare file name or the relative path.
      expect(text(q<HTMLElement>(fixture.nativeElement, ".file-title-main"))).toBe(
        `/dataset/${OWNER}/ds/v1/nested/b.csv`
      );
      // 2048 bytes reaches the reader as a human-readable size, not as a raw count.
      expect(text(q<HTMLElement>(fixture.nativeElement, ".file-size"))).toBe("2.00 KB");
    });

    it("deletes the file the tree asked to remove", () => {
      tree().triggerEventHandler("deletedTreeNode", leaf("b.csv"));

      expect(datasetService.deleteDatasetFile).toHaveBeenCalledWith(5, "nested/b.csv");
    });

    it("offers the tree's write controls only to a writer", () => {
      render({ userDatasetAccessLevel: "WRITE" });
      expect(tree().componentInstance.isTreeNodeDeletable).toBe(true);
      expect(tree().componentInstance.isCoverSettable).toBe(true);

      render({ userDatasetAccessLevel: "READ" });
      expect(tree().componentInstance.isTreeNodeDeletable).toBe(false);
      expect(tree().componentInstance.isCoverSettable).toBe(false);
    });

    it("adopts the cover image the tree offered, qualified by the selected version", () => {
      tree().triggerEventHandler("setCoverImage", "nested/b.png");

      expect(datasetService.updateDatasetCoverImage).toHaveBeenCalledWith(5, "v1/nested/b.png");
    });
  });

  describe("settings tab", () => {
    it("persists a description edited on the Settings tab", () => {
      render({ did: 5, userDatasetAccessLevel: "WRITE", datasetDescription: "old" });
      openTab("Settings");

      const editor = fixture.debugElement.query(By.css(".settings-field texera-markdown-description"));
      // The editor is what the writer types into, so it has to arrive holding the
      // description that is live and unlocked for editing. (The tab itself is
      // behind *ngIf="userHasWriteAccess()", so a reader never gets this far and
      // the read-only leg of [editable] is unreachable from here.)
      expect(editor.componentInstance.description).toBe("old");
      expect(editor.componentInstance.editable).toBe(true);

      editor.triggerEventHandler("descriptionChange", "brand new");

      expect(datasetService.updateDatasetDescription).toHaveBeenCalledWith(5, "brand new");
    });

    it("deletes the dataset only once the confirmation is accepted", () => {
      const el = render({ did: 5, datasetName: "ds", userDatasetAccessLevel: "WRITE", isOwner: true });
      openTab("Settings");
      const navigate = vi.spyOn(TestBed.inject(Router), "navigate").mockResolvedValue(true);

      q<HTMLButtonElement>(el, 'button[title="Delete"]').click();
      flush();
      expect(datasetService.deleteDatasets).not.toHaveBeenCalled();

      q<HTMLButtonElement>(overlay(), ".ant-popover-buttons button.ant-btn-primary").click();
      flush();

      expect(datasetService.deleteDatasets).toHaveBeenCalledWith(5);
      expect(navigate).toHaveBeenCalledWith([USER_DATASET]);
    });
  });
});
