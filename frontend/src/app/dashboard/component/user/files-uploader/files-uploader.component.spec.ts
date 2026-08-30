/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { of, Subject, throwError } from "rxjs";
import { OnDestroy } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { NgxFileDropComponent, NgxFileDropEntry } from "ngx-file-drop";
import { NzAlertComponent } from "ng-zorro-antd/alert";
import { NzModalService } from "ng-zorro-antd/modal";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { AdminSettingsService } from "../../../service/admin/settings/admin-settings.service";
import { DatasetService } from "../../../service/user/dataset/dataset.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { FileUploadItem } from "../../../type/dashboard-file.interface";
import { FilesUploaderComponent } from "./files-uploader.component";

interface CapturedModal {
  nzTitle: string;
  nzData: {
    path: string;
    hint?: string;
  };
  nzFooter: Array<{
    label: string;
    onClick: () => void;
  }>;
}

const waitUntil = async (condition: () => boolean): Promise<void> => {
  for (let i = 0; i < 20; i++) {
    if (condition()) return;
    await new Promise(resolve => setTimeout(resolve, 0));
  }
  throw new Error("condition was not met");
};

const droppedFile = (relativePath: string, file: File): NgxFileDropEntry =>
  ({
    relativePath,
    fileEntry: {
      isFile: true,
      file: (success: (file: File) => void): void => success(file),
    },
  }) as unknown as NgxFileDropEntry;

// A dropped folder: ngx-file-drop reports it as an entry whose fileEntry is not a file.
const droppedDirectory = (relativePath: string): NgxFileDropEntry =>
  ({
    relativePath,
    fileEntry: {
      isFile: false,
    },
  }) as unknown as NgxFileDropEntry;

describe("FilesUploaderComponent", () => {
  let component: FilesUploaderComponent;
  let modals: CapturedModal[];
  let datasetService: {
    listMultipartUploads: ReturnType<typeof vi.fn>;
    findExistingUploadFiles: ReturnType<typeof vi.fn>;
  };

  beforeEach(() => {
    modals = [];
    const modal = {
      create: vi.fn(config => {
        modals.push(config as CapturedModal);
        return { destroy: vi.fn() };
      }),
    } as unknown as NzModalService;
    const adminSettingsService = {
      getPublicSetting: vi.fn().mockReturnValue(of("20")),
    } as unknown as AdminSettingsService;
    datasetService = {
      listMultipartUploads: vi.fn().mockReturnValue(of(["failed.csv"])),
      findExistingUploadFiles: vi.fn().mockReturnValue(of(["done.csv"])),
    };

    component = new FilesUploaderComponent(
      { error: vi.fn() } as unknown as NotificationService,
      adminSettingsService,
      datasetService as unknown as DatasetService,
      modal
    );
    component.ownerEmail = "owner@example.com";
    component.datasetName = "dataset";
    component.did = 7;
  });

  it("keeps the default upload size limit when the public setting is missing, and parses it when present", () => {
    const build = (value: string | null) =>
      new FilesUploaderComponent(
        { error: vi.fn() } as unknown as NotificationService,
        { getPublicSetting: vi.fn().mockReturnValue(of(value)) } as unknown as AdminSettingsService,
        datasetService as unknown as DatasetService,
        { create: vi.fn() } as unknown as NzModalService
      );

    expect(build(null).singleFileUploadMaxSizeMiB).toBe(20);
    expect(build("128").singleFileUploadMaxSizeMiB).toBe(128);
    // an unparsable value keeps the default, but a stored 0 is honoured
    expect(build("nope").singleFileUploadMaxSizeMiB).toBe(20);
    expect(build("0").singleFileUploadMaxSizeMiB).toBe(0);
  });

  it("keeps the default upload size limit when the setting request fails", () => {
    // The component swallows the error on purpose so a settings outage cannot stop uploads.
    const uploader = new FilesUploaderComponent(
      { error: vi.fn() } as unknown as NotificationService,
      {
        getPublicSetting: vi.fn().mockReturnValue(throwError(() => new Error("settings unavailable"))),
      } as unknown as AdminSettingsService,
      datasetService as unknown as DatasetService,
      { create: vi.fn() } as unknown as NzModalService
    );

    expect(uploader.singleFileUploadMaxSizeMiB).toBe(20);
  });

  it("asks to resume failed multipart files and skip completed matching files in one retry batch", async () => {
    const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

    component.fileDropped([
      droppedFile("failed.csv", new File(["half"], "failed.csv")),
      droppedFile("done.csv", new File(["done"], "done.csv")),
    ]);

    await waitUntil(() => modals.length === 1);
    expect(modals[0].nzTitle).toBe("Conflicting File");
    expect(modals[0].nzData.path).toBe("failed.csv");
    modals[0].nzFooter.find(button => button.label === "Resume")?.onClick();

    await waitUntil(() => modals.length === 2);
    expect(modals[1].nzTitle).toBe("Matching File Found");
    expect(modals[1].nzData.path).toBe("done.csv");
    expect(modals[1].nzData.hint).toContain("same path and size");
    modals[1].nzFooter.find(button => button.label === "Skip")?.onClick();

    expect((await emitted).map(item => item.name)).toEqual(["failed.csv"]);
  });

  it("asks both questions when the same file has an active upload session and an existing match", async () => {
    datasetService.listMultipartUploads.mockReturnValue(of(["same.csv"]));
    datasetService.findExistingUploadFiles.mockReturnValue(of(["same.csv"]));
    const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

    component.fileDropped([droppedFile("same.csv", new File(["same"], "same.csv"))]);

    await waitUntil(() => modals.length === 1);
    expect(modals[0].nzTitle).toBe("Conflicting File");
    expect(modals[0].nzData.path).toBe("same.csv");
    modals[0].nzFooter.find(button => button.label === "Resume")?.onClick();

    await waitUntil(() => modals.length === 2);
    expect(modals[1].nzTitle).toBe("Matching File Found");
    expect(modals[1].nzData.path).toBe("same.csv");
    modals[1].nzFooter.find(button => button.label === "Upload")?.onClick();

    expect((await emitted).map(item => item.name)).toEqual(["same.csv"]);
  });

  /**
   * The Restart choices are the half of the conflict dialog the existing tests never take. They
   * differ from Resume by exactly one observable: `item.restart`, which flips from its default
   * `false` to `true` and makes the uploader call the backend with type=forceRestart instead of
   * continuing the existing multipart session. A Resume/Restart mix-up therefore silently resumes a
   * session the user asked to discard.
   */
  it("marks a file for force-restart when Restart is chosen", async () => {
    datasetService.findExistingUploadFiles.mockReturnValue(of([]));
    const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

    component.fileDropped([droppedFile("failed.csv", new File(["half"], "failed.csv"))]);

    await waitUntil(() => modals.length === 1);
    modals[0].nzFooter.find(button => button.label === "Restart")?.onClick();

    const items = await emitted;
    expect(items.map(item => item.name)).toEqual(["failed.csv"]);
    expect(items[0].restart).toBe(true);
  });

  it("leaves the restart flag unset when Resume is chosen", async () => {
    // The counterpart of the test above: same file, other button. Without this pair, a
    // markForceRestart call added to the Resume branch would go unnoticed.
    datasetService.findExistingUploadFiles.mockReturnValue(of([]));
    const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

    component.fileDropped([droppedFile("failed.csv", new File(["half"], "failed.csv"))]);

    await waitUntil(() => modals.length === 1);
    modals[0].nzFooter.find(button => button.label === "Resume")?.onClick();

    const items = await emitted;
    expect(items.map(item => item.name)).toEqual(["failed.csv"]);
    expect(items[0].restart).toBeFalsy();
  });

  it("restarts every remaining conflicting file after one Restart For All choice", async () => {
    datasetService.listMultipartUploads.mockReturnValue(of(["one.csv", "two.csv"]));
    datasetService.findExistingUploadFiles.mockReturnValue(of([]));
    const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

    component.fileDropped([
      droppedFile("one.csv", new File(["one"], "one.csv")),
      droppedFile("two.csv", new File(["two"], "two.csv")),
    ]);

    await waitUntil(() => modals.length === 1);
    modals[0].nzFooter.find(button => button.label === "Restart For All")?.onClick();

    const items = await emitted;
    expect(items.map(item => item.name)).toEqual(["one.csv", "two.csv"]);
    // Both files carry the flag, and the second one never prompted - the "For All" latch has to
    // apply the restart itself rather than just suppressing the dialog.
    expect(items.map(item => item.restart)).toEqual([true, true]);
    expect(modals).toHaveLength(1);
  });

  it("resumes every remaining conflicting file after one Resume For All choice", async () => {
    datasetService.listMultipartUploads.mockReturnValue(of(["one.csv", "two.csv"]));
    datasetService.findExistingUploadFiles.mockReturnValue(of([]));
    const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

    component.fileDropped([
      droppedFile("one.csv", new File(["one"], "one.csv")),
      droppedFile("two.csv", new File(["two"], "two.csv")),
    ]);

    await waitUntil(() => modals.length === 1);
    modals[0].nzFooter.find(button => button.label === "Resume For All")?.onClick();

    const items = await emitted;
    expect(items.map(item => item.name)).toEqual(["one.csv", "two.csv"]);
    // Distinguishes the two latches: resumeAll must NOT set the flag restartAll sets.
    expect(items.every(item => !item.restart)).toBe(true);
    expect(modals).toHaveLength(1);
  });

  it("passes a non-conflicting file straight through without prompting", async () => {
    datasetService.listMultipartUploads.mockReturnValue(of(["other.csv"]));
    datasetService.findExistingUploadFiles.mockReturnValue(of([]));
    const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

    component.fileDropped([droppedFile("clean.csv", new File(["clean"], "clean.csv"))]);

    const items = await emitted;
    expect(items.map(item => item.name)).toEqual(["clean.csv"]);
    expect(items[0].restart).toBeFalsy();
    expect(modals).toHaveLength(0);
  });

  it("skips all matching files after one Skip For All choice", async () => {
    datasetService.listMultipartUploads.mockReturnValue(of([]));
    datasetService.findExistingUploadFiles.mockReturnValue(of(["one.csv", "two.csv"]));
    const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

    component.fileDropped([
      droppedFile("one.csv", new File(["one"], "one.csv")),
      droppedFile("two.csv", new File(["two"], "two.csv")),
    ]);

    await waitUntil(() => modals.length === 1);
    expect(modals[0].nzData.path).toBe("one.csv");
    modals[0].nzFooter.find(button => button.label === "Skip For All")?.onClick();

    expect(await emitted).toEqual([]);
    expect(modals).toHaveLength(1);
    expect(component.fileUploadBannerType).toBe("info");
    expect(component.fileUploadBannerMessage).toContain("2 matching files were skipped.");
  });

  it("uploads all matching files after one Upload For All choice", async () => {
    datasetService.listMultipartUploads.mockReturnValue(of([]));
    datasetService.findExistingUploadFiles.mockReturnValue(of(["one.csv", "two.csv"]));
    const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

    component.fileDropped([
      droppedFile("one.csv", new File(["one"], "one.csv")),
      droppedFile("two.csv", new File(["two"], "two.csv")),
    ]);

    await waitUntil(() => modals.length === 1);
    expect(modals[0].nzData.path).toBe("one.csv");
    modals[0].nzFooter.find(button => button.label === "Upload For All")?.onClick();

    expect((await emitted).map(item => item.name)).toEqual(["one.csv", "two.csv"]);
    expect(modals).toHaveLength(1);
    expect(component.fileUploadBannerType).toBe("success");
  });

  describe("dropped folders are not failures", () => {
    // A folder resolves to null rather than rejecting, so it must not be counted as a
    // file that failed to be selected.
    beforeEach(() => {
      datasetService.listMultipartUploads.mockReturnValue(of([]));
      datasetService.findExistingUploadFiles.mockReturnValue(of([]));
    });

    const rejectEveryFile = () => (component.singleFileUploadMaxSizeMiB = 0);

    it("shows no banner at all when only a folder is dropped", async () => {
      const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

      component.fileDropped([droppedDirectory("some-folder")]);

      expect(await emitted).toEqual([]);
      expect(component.fileUploadingFinished).toBe(false);
      expect(component.fileUploadBannerMessage).toBe("");
    });

    it("reports only the valid file when a folder is dropped alongside it", async () => {
      const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

      component.fileDropped([
        droppedDirectory("some-folder"),
        droppedFile("clean.csv", new File(["clean"], "clean.csv")),
      ]);

      expect((await emitted).map(item => item.name)).toEqual(["clean.csv"]);
      expect(component.fileUploadBannerType).toBe("success");
      expect(component.fileUploadBannerMessage).toContain("1 file selected successfully!");
    });

    it("still reports a file that genuinely failed to be selected", async () => {
      rejectEveryFile();
      const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

      component.fileDropped([droppedFile("huge.csv", new File(["too big"], "huge.csv"))]);

      expect(await emitted).toEqual([]);
      expect(component.fileUploadBannerType).toBe("error");
      expect(component.fileUploadBannerMessage).toBe("1 file failed to be selected.");
    });

    it("counts only the rejected file when a folder is dropped with it", async () => {
      rejectEveryFile();
      const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

      component.fileDropped([
        droppedDirectory("some-folder"),
        droppedFile("huge.csv", new File(["too big"], "huge.csv")),
      ]);

      expect(await emitted).toEqual([]);
      expect(component.fileUploadBannerMessage).toBe("1 file failed to be selected.");
    });

    it("keeps the plural wording when several files are rejected", async () => {
      rejectEveryFile();
      const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

      component.fileDropped([
        droppedDirectory("some-folder"),
        droppedFile("huge1.csv", new File(["too big"], "huge1.csv")),
        droppedFile("huge2.csv", new File(["too big"], "huge2.csv")),
      ]);

      expect(await emitted).toEqual([]);
      expect(component.fileUploadBannerMessage).toBe("2 files failed to be selected.");
    });
  });

  it("showFileUploadBanner marks uploading finished and stores the given type and message", () => {
    // preconditions: fresh component starts hidden with the default "success" type and empty message
    expect(component.fileUploadingFinished).toBe(false);
    expect(component.fileUploadBannerType).toBe("success");
    expect(component.fileUploadBannerMessage).toBe("");

    component.showFileUploadBanner("error", "3 files failed to be selected.");

    expect(component.fileUploadingFinished).toBe(true);
    expect(component.fileUploadBannerType).toBe("error");
    expect(component.fileUploadBannerMessage).toBe("3 files failed to be selected.");
  });

  it("showFileUploadBanner overwrites the previously shown banner on a second call", () => {
    component.showFileUploadBanner("error", "first message");
    component.showFileUploadBanner("info", "second message");

    expect(component.fileUploadingFinished).toBe(true);
    expect(component.fileUploadBannerType).toBe("info");
    expect(component.fileUploadBannerMessage).toBe("second message");
  });

  it("hideBanner clears only the finished flag, leaving the last banner type and message intact", () => {
    component.showFileUploadBanner("warning", "heads up");

    component.hideBanner();

    expect(component.fileUploadingFinished).toBe(false);
    // hideBanner resets visibility only; it does not touch type or message
    expect(component.fileUploadBannerType).toBe("warning");
    expect(component.fileUploadBannerMessage).toBe("heads up");
  });

  /**
   * Everything above drops well-formed files into a fully configured uploader. These cover what
   * happens when the drop itself, or the lookups the drop depends on, do not go to plan — the
   * paths that decide whether a file even reaches the upload queue.
   */
  describe("drops that do not yield an uploadable file", () => {
    /** Resolves with whatever the component finally emits for a drop. */
    const emissionOf = (): Promise<FileUploadItem[]> =>
      new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

    it("rejects a single oversized file and reports it in the banner", async () => {
      const notify = { error: vi.fn() };
      component = new FilesUploaderComponent(
        notify as unknown as NotificationService,
        { getPublicSetting: vi.fn().mockReturnValue(of("0")) } as unknown as AdminSettingsService,
        datasetService as unknown as DatasetService,
        { create: vi.fn() } as unknown as NzModalService
      );
      const emitted = emissionOf();

      component.fileDropped([droppedFile("big.csv", new File(["x"], "big.csv"))]);

      expect(await emitted).toEqual([]);
      expect(notify.error).toHaveBeenCalledWith("File big.csv's size exceeds the maximum limit of 0MiB.");
      expect(component.fileUploadBannerType).toBe("error");
      expect(component.fileUploadBannerMessage).toBe("1 file failed to be selected.");
    });

    it("pluralises the failure banner for more than one rejected file", async () => {
      component = new FilesUploaderComponent(
        { error: vi.fn() } as unknown as NotificationService,
        { getPublicSetting: vi.fn().mockReturnValue(of("0")) } as unknown as AdminSettingsService,
        datasetService as unknown as DatasetService,
        { create: vi.fn() } as unknown as NzModalService
      );
      const emitted = emissionOf();

      component.fileDropped([
        droppedFile("a.csv", new File(["x"], "a.csv")),
        droppedFile("b.csv", new File(["y"], "b.csv")),
      ]);

      expect(await emitted).toEqual([]);
      expect(component.fileUploadBannerMessage).toBe("2 files failed to be selected.");
    });

    it("ignores a dropped directory", async () => {
      const emitted = emissionOf();

      component.fileDropped([{ relativePath: "folder", fileEntry: { isFile: false } } as unknown as NgxFileDropEntry]);

      expect(await emitted).toEqual([]);
    });

    it("drops a file whose entry cannot be read", async () => {
      const failingEntry = {
        relativePath: "unreadable.csv",
        fileEntry: {
          isFile: true,
          file: (_success: (file: File) => void, failure: (error: unknown) => void): void =>
            failure(new Error("cannot read")),
        },
      } as unknown as NgxFileDropEntry;
      const emitted = emissionOf();

      component.fileDropped([failingEntry]);

      expect(await emitted).toEqual([]);
    });
  });

  describe("lookups the drop depends on", () => {
    const emissionOf = (): Promise<FileUploadItem[]> =>
      new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

    it("skips both lookups when the uploader has no dataset context", async () => {
      // The standalone (dataset-creation) usage: no owner/name and no did yet.
      component.ownerEmail = "";
      component.datasetName = "";
      component.did = undefined;
      const emitted = emissionOf();

      component.fileDropped([droppedFile("fresh.csv", new File(["new"], "fresh.csv"))]);

      expect((await emitted).map(item => item.name)).toEqual(["fresh.csv"]);
      expect(datasetService.listMultipartUploads).not.toHaveBeenCalled();
      expect(datasetService.findExistingUploadFiles).not.toHaveBeenCalled();
    });

    it("treats a failed lookup as nothing to reconcile", async () => {
      datasetService.listMultipartUploads.mockReturnValue(throwError(() => new Error("offline")));
      datasetService.findExistingUploadFiles.mockReturnValue(throwError(() => new Error("offline")));
      const emitted = emissionOf();

      component.fileDropped([droppedFile("failed.csv", new File(["half"], "failed.csv"))]);

      // No dialog can be raised without paths, so the file goes straight through.
      expect((await emitted).map(item => item.name)).toEqual(["failed.csv"]);
      expect(modals).toEqual([]);
    });

    it("treats a null lookup result as nothing to reconcile", async () => {
      datasetService.listMultipartUploads.mockReturnValue(of(null));
      datasetService.findExistingUploadFiles.mockReturnValue(of(null));
      const emitted = emissionOf();

      component.fileDropped([droppedFile("failed.csv", new File(["half"], "failed.csv"))]);

      expect((await emitted).map(item => item.name)).toEqual(["failed.csv"]);
      expect(modals).toEqual([]);
    });

    it("reports an unexpected failure of the whole drop", async () => {
      datasetService.listMultipartUploads.mockImplementation(() => {
        throw new Error("lookup exploded");
      });

      component.fileDropped([droppedFile("any.csv", new File(["x"], "any.csv"))]);

      await waitUntil(() => component.fileUploadingFinished);
      expect(component.fileUploadBannerType).toBe("error");
      expect(component.fileUploadBannerMessage).toBe("Unexpected error: lookup exploded");
    });

    it("reports an unexpected failure that carries no message", async () => {
      datasetService.listMultipartUploads.mockImplementation(() => {
        throw "lookup exploded";
      });

      component.fileDropped([droppedFile("any.csv", new File(["x"], "any.csv"))]);

      await waitUntil(() => component.fileUploadingFinished);
      expect(component.fileUploadBannerMessage).toBe("Unexpected error: lookup exploded");
    });
  });

  it("stops tracking the size setting once destroyed", () => {
    // @UntilDestroy() supplies the ngOnDestroy that ends the `untilDestroyed(this)`
    // subscription; without it a late setting would still be applied to a dead component.
    const setting = new Subject<string>();
    const uploader = new FilesUploaderComponent(
      { error: vi.fn() } as unknown as NotificationService,
      { getPublicSetting: vi.fn().mockReturnValue(setting) } as unknown as AdminSettingsService,
      datasetService as unknown as DatasetService,
      { create: vi.fn() } as unknown as NzModalService
    );

    setting.next("50");
    expect(uploader.singleFileUploadMaxSizeMiB).toBe(50);

    (uploader as unknown as OnDestroy).ngOnDestroy();
    setting.next("99");

    expect(uploader.singleFileUploadMaxSizeMiB).toBe(50);
  });

  describe("dialog titles for paths without a file name", () => {
    it("falls back to the whole path when the conflicting path ends in a separator", async () => {
      datasetService.listMultipartUploads.mockReturnValue(of(["folder/"]));
      datasetService.findExistingUploadFiles.mockReturnValue(of(["folder/"]));
      const emitted = new Promise<FileUploadItem[]>(resolve => component.uploadedFiles.subscribe(resolve));

      component.fileDropped([droppedFile("folder/", new File(["x"], "x"))]);

      await waitUntil(() => modals.length === 1);
      expect(modals[0].nzData.path).toBe("folder/");
      modals[0].nzFooter.find(button => button.label === "Resume")?.onClick();

      await waitUntil(() => modals.length === 2);
      expect(modals[1].nzData.path).toBe("folder/");
      modals[1].nzFooter.find(button => button.label === "Upload")?.onClick();

      expect((await emitted).map(item => item.name)).toEqual(["folder/"]);
    });
  });
});

/**
 * The suite above constructs the component directly, so its template has never been
 * rendered — the banner's `*ngIf`, the banner bindings and the drop-zone button live
 * only in the template. These mount it for real.
 */
describe("FilesUploaderComponent rendered", () => {
  let fixture: ComponentFixture<FilesUploaderComponent>;
  let component: FilesUploaderComponent;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [FilesUploaderComponent, NoopAnimationsModule],
      providers: [
        { provide: NotificationService, useValue: { error: vi.fn() } },
        { provide: AdminSettingsService, useValue: { getPublicSetting: vi.fn().mockReturnValue(of("20")) } },
        {
          provide: DatasetService,
          useValue: {
            listMultipartUploads: vi.fn().mockReturnValue(of([])),
            findExistingUploadFiles: vi.fn().mockReturnValue(of([])),
          },
        },
        { provide: NzModalService, useValue: { create: vi.fn() } },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(FilesUploaderComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  const alert = (): HTMLElement | null => (fixture.nativeElement as HTMLElement).querySelector("nz-alert");

  it("hides the banner until the alert is enabled and the upload has finished", () => {
    expect(alert()).toBeNull();

    component.showUploadAlert = true;
    fixture.detectChanges();
    expect(alert()).toBeNull();

    component.showUploadAlert = false;
    component.fileUploadingFinished = true;
    fixture.detectChanges();
    expect(alert()).toBeNull();
  });

  it("renders the banner message once both flags are set", () => {
    component.showUploadAlert = true;
    component.fileUploadingFinished = true;
    component.fileUploadBannerType = "error";
    component.fileUploadBannerMessage = "Upload failed. Please retry.";
    fixture.detectChanges();

    const banner = alert();
    expect(banner).not.toBeNull();
    expect(banner!.textContent).toContain("Upload failed. Please retry.");
  });

  it("clears the banner when its close control fires", () => {
    component.showUploadAlert = true;
    component.fileUploadingFinished = true;
    component.fileUploadBannerMessage = "done";
    fixture.detectChanges();

    fixture.debugElement.query(By.directive(NzAlertComponent)).componentInstance.nzOnClose.emit();
    fixture.detectChanges();

    expect(component.fileUploadingFinished).toBe(false);
    expect(alert()).toBeNull();
  });

  it("opens the file selector from the drop-zone button", () => {
    // ngx-file-drop hands its `openFileSelector` to the content template by reference,
    // so spying on the component's property after render would not be seen. Assert its
    // effect instead: it clicks the hidden file input.
    const host = fixture.nativeElement as HTMLElement;
    const fileInput: HTMLInputElement = host.querySelector("input.ngx-file-drop__file-input")!;
    expect(fileInput).not.toBeNull();
    const openDialog = vi.spyOn(fileInput, "click").mockImplementation(() => {});

    const button: HTMLButtonElement = host.querySelector(".upload-file-button")!;
    expect(button).not.toBeNull();
    button.click();

    expect(openDialog).toHaveBeenCalled();
  });

  it("routes a drop on the zone into fileDropped", () => {
    const dropped = vi.spyOn(component, "fileDropped").mockImplementation(() => {});
    const entries = [droppedFile("a.csv", new File(["a"], "a.csv"))];

    fixture.debugElement.query(By.directive(NgxFileDropComponent)).componentInstance.onFileDrop.emit(entries);

    expect(dropped).toHaveBeenCalledWith(entries);
  });
});
