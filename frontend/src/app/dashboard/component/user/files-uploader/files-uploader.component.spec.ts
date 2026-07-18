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

import { of } from "rxjs";
import { NgxFileDropEntry } from "ngx-file-drop";
import { NzModalService } from "ng-zorro-antd/modal";
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
});
