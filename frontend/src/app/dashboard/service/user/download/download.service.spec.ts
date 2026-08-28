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

import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { DownloadService, EXPORT_BASE_URL } from "./download.service";
import { DatasetService } from "../dataset/dataset.service";
import { ModelService } from "../model/model.service";
import { FileSaverService } from "../file/file-saver.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { firstValueFrom, lastValueFrom, of, throwError } from "rxjs";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import type { Mocked } from "vitest";
import { WORKFLOW_EXECUTIONS_API_BASE_URL } from "../workflow-executions/workflow-executions.service";
import { DashboardWorkflowComputingUnit } from "../../../../common/type/workflow-computing-unit";
import JSZip from "jszip";

function computingUnit(type: string, cuid: number): DashboardWorkflowComputingUnit {
  return { computingUnit: { cuid, type } } as unknown as DashboardWorkflowComputingUnit;
}
const EXPORT_OPERATORS = [{ id: "op1", outputType: "csv" }];

describe("DownloadService", () => {
  let downloadService: DownloadService;
  let datasetServiceSpy: Mocked<DatasetService>;
  let modelServiceSpy: Mocked<ModelService>;
  let fileSaverServiceSpy: Mocked<FileSaverService>;
  let notificationServiceSpy: Mocked<NotificationService>;
  let workflowPersistServiceSpy: Mocked<WorkflowPersistService>;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    const datasetSpy = { retrieveDatasetVersionSingleFile: vi.fn(), retrieveDatasetVersionZip: vi.fn() };
    const modelSpy = { retrieveModelVersionSingleFile: vi.fn(), retrieveModelVersionZip: vi.fn() };
    const fileSaverSpy = { saveAs: vi.fn() };
    const notificationSpy = { info: vi.fn(), success: vi.fn(), error: vi.fn() };
    const workflowPersistSpy = { retrieveWorkflow: vi.fn() };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        DownloadService,
        { provide: DatasetService, useValue: datasetSpy },
        { provide: ModelService, useValue: modelSpy },
        { provide: FileSaverService, useValue: fileSaverSpy },
        { provide: NotificationService, useValue: notificationSpy },
        { provide: WorkflowPersistService, useValue: workflowPersistSpy },
        ...commonTestProviders,
      ],
    });

    downloadService = TestBed.inject(DownloadService);
    datasetServiceSpy = TestBed.inject(DatasetService) as unknown as Mocked<DatasetService>;
    modelServiceSpy = TestBed.inject(ModelService) as unknown as Mocked<ModelService>;
    fileSaverServiceSpy = TestBed.inject(FileSaverService) as unknown as Mocked<FileSaverService>;
    notificationServiceSpy = TestBed.inject(NotificationService) as unknown as Mocked<NotificationService>;
    workflowPersistServiceSpy = TestBed.inject(WorkflowPersistService) as unknown as Mocked<WorkflowPersistService>;
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    // Catch any test that fires an HTTP request without flushing it; keeps
    // the suite safe as more specs start using HttpTestingController.
    httpMock.verify();
  });

  // ─── downloadSingleFile ───────────────────────────────────────────────────

  it("downloads a single file and saves it under the basename of the path", async () => {
    const mockBlob = new Blob(["test content"], { type: "text/plain" });
    datasetServiceSpy.retrieveDatasetVersionSingleFile.mockReturnValue(of(mockBlob));

    const result = await firstValueFrom(downloadService.downloadSingleFile("test/file.txt", true));

    expect(result).toBe(mockBlob);
    expect(notificationServiceSpy.info).toHaveBeenCalledWith("Starting to download file test/file.txt");
    expect(datasetServiceSpy.retrieveDatasetVersionSingleFile).toHaveBeenCalledWith("test/file.txt", true);
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(mockBlob, "file.txt");
    expect(notificationServiceSpy.success).toHaveBeenCalledWith("File test/file.txt has been downloaded");
  });

  it("falls back to a default filename when the path has no basename segment", async () => {
    const mockBlob = new Blob(["x"], { type: "text/plain" });
    datasetServiceSpy.retrieveDatasetVersionSingleFile.mockReturnValue(of(mockBlob));

    await firstValueFrom(downloadService.downloadSingleFile("", true));

    // path.split("/").pop() returns "" for "", which falls through to the default name
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(mockBlob, "download");
  });

  it("propagates errors from downloadSingleFile and emits the error notification", async () => {
    datasetServiceSpy.retrieveDatasetVersionSingleFile.mockReturnValue(throwError(() => new Error("boom")));

    await expect(firstValueFrom(downloadService.downloadSingleFile("test/file.txt", true))).rejects.toThrow("boom");

    expect(notificationServiceSpy.info).toHaveBeenCalledWith("Starting to download file test/file.txt");
    expect(fileSaverServiceSpy.saveAs).not.toHaveBeenCalled();
    expect(notificationServiceSpy.error).toHaveBeenCalledWith("Error downloading file 'test/file.txt'");
  });

  it("passes isLogin=false through to retrieveDatasetVersionSingleFile", async () => {
    const mockBlob = new Blob(["x"], { type: "text/plain" });
    datasetServiceSpy.retrieveDatasetVersionSingleFile.mockReturnValue(of(mockBlob));

    await firstValueFrom(downloadService.downloadSingleFile("public/sample.csv", false));

    expect(datasetServiceSpy.retrieveDatasetVersionSingleFile).toHaveBeenCalledWith("public/sample.csv", false);
  });

  // ─── downloadDataset ──────────────────────────────────────────────────────

  it("downloads the latest dataset version as a zip named after the dataset", async () => {
    const mockBlob = new Blob(["dataset content"], { type: "application/zip" });
    datasetServiceSpy.retrieveDatasetVersionZip.mockReturnValue(of(mockBlob));

    const result = await firstValueFrom(downloadService.downloadDataset(1, "TestDataset"));

    expect(result).toBe(mockBlob);
    expect(notificationServiceSpy.info).toHaveBeenCalledWith(
      "Starting to download the latest version of the dataset as ZIP"
    );
    expect(datasetServiceSpy.retrieveDatasetVersionZip).toHaveBeenCalledWith(1);
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(mockBlob, "TestDataset.zip");
    expect(notificationServiceSpy.success).toHaveBeenCalledWith(
      "The latest version of the dataset has been downloaded as ZIP"
    );
  });

  it("emits the dataset error notification and rethrows on retrieve failure", async () => {
    datasetServiceSpy.retrieveDatasetVersionZip.mockReturnValue(throwError(() => new Error("fail")));

    await expect(firstValueFrom(downloadService.downloadDataset(1, "TestDataset"))).rejects.toThrow("fail");

    expect(fileSaverServiceSpy.saveAs).not.toHaveBeenCalled();
    expect(notificationServiceSpy.error).toHaveBeenCalledWith(
      "Error downloading the latest version of the dataset as ZIP"
    );
  });

  // ─── downloadDatasetVersion ───────────────────────────────────────────────

  it("downloads a specific dataset version with composite zip name", async () => {
    const mockBlob = new Blob(["v1"], { type: "application/zip" });
    datasetServiceSpy.retrieveDatasetVersionZip.mockReturnValue(of(mockBlob));

    const result = await firstValueFrom(downloadService.downloadDatasetVersion(1, 2, "TestDataset", "v1.0"));

    expect(result).toBe(mockBlob);
    expect(notificationServiceSpy.info).toHaveBeenCalledWith("Starting to download version v1.0 as ZIP");
    expect(datasetServiceSpy.retrieveDatasetVersionZip).toHaveBeenCalledWith(1, 2);
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(mockBlob, "TestDataset-v1.0.zip");
    expect(notificationServiceSpy.success).toHaveBeenCalledWith("Version v1.0 has been downloaded as ZIP");
  });

  it("emits the version-specific error notification on retrieve failure", async () => {
    datasetServiceSpy.retrieveDatasetVersionZip.mockReturnValue(throwError(() => new Error("nope")));

    await expect(firstValueFrom(downloadService.downloadDatasetVersion(1, 2, "TestDataset", "v1.0"))).rejects.toThrow(
      "nope"
    );

    expect(notificationServiceSpy.error).toHaveBeenCalledWith("Error downloading version 'v1.0' as ZIP");
  });

  // ─── model downloads ──────────────────────────────────────────────────────

  it("downloads a model's latest version, a chosen version, and a single file", async () => {
    const zip = new Blob(["model"], { type: "application/zip" });
    const file = new Blob(["weights"]);
    modelServiceSpy.retrieveModelVersionZip.mockReturnValue(of(zip));
    modelServiceSpy.retrieveModelVersionSingleFile.mockReturnValue(of(file));

    expect(await firstValueFrom(downloadService.downloadModel(4, "resnet-50"))).toBe(zip);
    expect(modelServiceSpy.retrieveModelVersionZip).toHaveBeenCalledWith(4);
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(zip, "resnet-50.zip");

    expect(await firstValueFrom(downloadService.downloadModelVersion(4, 2, "resnet-50", "v2"))).toBe(zip);
    expect(modelServiceSpy.retrieveModelVersionZip).toHaveBeenCalledWith(4, 2);
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(zip, "resnet-50-v2.zip");

    expect(await firstValueFrom(downloadService.downloadModelSingleFile("/models/a/m/v2/model.pt"))).toBe(file);
    expect(modelServiceSpy.retrieveModelVersionSingleFile).toHaveBeenCalledWith("/models/a/m/v2/model.pt", true);
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(file, "model.pt");
  });

  it("passes the logged-out flag through to the model file endpoint", async () => {
    modelServiceSpy.retrieveModelVersionSingleFile.mockReturnValue(of(new Blob()));

    await firstValueFrom(downloadService.downloadModelSingleFile("/models/a/m/v2/model.pt", false));

    expect(modelServiceSpy.retrieveModelVersionSingleFile).toHaveBeenCalledWith("/models/a/m/v2/model.pt", false);
  });

  it("emits the model error notification and rethrows on retrieve failure", async () => {
    modelServiceSpy.retrieveModelVersionZip.mockReturnValue(throwError(() => new Error("fail")));

    await expect(firstValueFrom(downloadService.downloadModel(4, "resnet-50"))).rejects.toThrow("fail");

    expect(fileSaverServiceSpy.saveAs).not.toHaveBeenCalled();
    expect(notificationServiceSpy.error).toHaveBeenCalledWith(
      "Error downloading the latest version of the model as ZIP"
    );
  });

  // ─── downloadWorkflow ─────────────────────────────────────────────────────

  it("downloads a workflow as a JSON blob named after the workflow", async () => {
    const workflowContent = { hello: "world", operators: [] };
    workflowPersistServiceSpy.retrieveWorkflow.mockReturnValue(of({ content: workflowContent } as any));

    const result = await firstValueFrom(downloadService.downloadWorkflow(42, "MyWorkflow"));

    expect(result.fileName).toBe("MyWorkflow.json");
    expect(result.blob).toBeInstanceOf(Blob);
    expect(result.blob.type).toBe("text/plain;charset=utf-8");
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(result.blob, "MyWorkflow.json");
    // Blob.text() isn't shipped by jsdom, so we don't pin the body content
    // here; the saveAs assertion above already verifies the path that
    // produced it.
  });

  // ─── downloadWorkflowsAsZip ───────────────────────────────────────────────

  it("downloads the workflow ZIP and routes through createWorkflowsZip", async () => {
    const mockBlob = new Blob(["zip"], { type: "application/zip" });
    const entries = [
      { id: 1, name: "Workflow1" },
      { id: 2, name: "Workflow2" },
    ];
    vi.spyOn(downloadService as any, "createWorkflowsZip").mockReturnValue(of(mockBlob));

    const result = await firstValueFrom(downloadService.downloadWorkflowsAsZip(entries));

    expect(result).toBe(mockBlob);
    expect(notificationServiceSpy.info).toHaveBeenCalledWith("Starting to download workflows as ZIP");
    expect((downloadService as any).createWorkflowsZip).toHaveBeenCalledWith(entries);
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(
      mockBlob,
      expect.stringMatching(/^workflowExports-.*\.zip$/)
    );
    expect(notificationServiceSpy.success).toHaveBeenCalledWith("Workflows have been downloaded as ZIP");
  });

  it("propagates errors from createWorkflowsZip with the expected error notification", async () => {
    vi.spyOn(downloadService as any, "createWorkflowsZip").mockReturnValue(throwError(() => new Error("zip fail")));

    await expect(firstValueFrom(downloadService.downloadWorkflowsAsZip([{ id: 1, name: "W" }]))).rejects.toThrow(
      "zip fail"
    );

    expect(fileSaverServiceSpy.saveAs).not.toHaveBeenCalled();
    expect(notificationServiceSpy.error).toHaveBeenCalledWith("Error downloading workflows as ZIP");
  });

  // ─── createWorkflowsZip / nameWorkflow (real zip assembly) ────────────────
  // These drive downloadWorkflowsAsZip through the real (un-mocked) private
  // createWorkflowsZip → retrieveWorkflowItem → nameWorkflow chain, so the produced
  // blob is a genuine zip we can load back and inspect.

  it("assembles a real zip with one JSON entry per workflow", async () => {
    workflowPersistServiceSpy.retrieveWorkflow.mockReturnValue(of({ content: { op: "x" } } as any));

    const result = await firstValueFrom(
      downloadService.downloadWorkflowsAsZip([
        { id: 1, name: "Alpha" },
        { id: 2, name: "Beta" },
      ])
    );

    expect(result).toBeInstanceOf(Blob);
    expect(workflowPersistServiceSpy.retrieveWorkflow).toHaveBeenCalledWith(1);
    expect(workflowPersistServiceSpy.retrieveWorkflow).toHaveBeenCalledWith(2);
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(result, expect.stringMatching(/^workflowExports-.*\.zip$/));
    expect(notificationServiceSpy.success).toHaveBeenCalledWith("Workflows have been downloaded as ZIP");

    const loaded = await JSZip.loadAsync(result);
    expect(Object.keys(loaded.files).sort()).toEqual(["Alpha.json", "Beta.json"]);
  });

  it("de-duplicates colliding workflow filenames inside the zip", async () => {
    workflowPersistServiceSpy.retrieveWorkflow.mockReturnValue(of({ content: { op: "x" } } as any));

    const result = await firstValueFrom(
      downloadService.downloadWorkflowsAsZip([
        { id: 1, name: "Dup" },
        { id: 2, name: "Dup" },
        { id: 3, name: "Dup" },
      ])
    );

    // nameWorkflow appends -1, -2, ... on each collision so no entry is lost.
    const loaded = await JSZip.loadAsync(result);
    expect(Object.keys(loaded.files).sort()).toEqual(["Dup-1.json", "Dup-2.json", "Dup.json"]);
  });

  // ─── zip download must not also save each workflow individually ───────────
  // Regression: createWorkflowsZip used to reuse downloadWorkflow purely to obtain
  // the blob, and downloadWorkflow also saves to disk, so a zip of N workflows
  // wrote N+1 files.

  it("saves only the zip, not one JSON per workflow, when several workflows are zipped", async () => {
    workflowPersistServiceSpy.retrieveWorkflow.mockReturnValue(of({ content: { op: "x" } } as any));

    const result = await firstValueFrom(
      downloadService.downloadWorkflowsAsZip([
        { id: 1, name: "Alpha" },
        { id: 2, name: "Beta" },
        { id: 3, name: "Gamma" },
      ])
    );

    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledTimes(1);
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(result, expect.stringMatching(/^workflowExports-.*\.zip$/));

    const savedNames = fileSaverServiceSpy.saveAs.mock.calls.map(([, fileName]) => fileName);
    expect(savedNames).not.toContain("Alpha.json");
    expect(savedNames).not.toContain("Beta.json");
    expect(savedNames).not.toContain("Gamma.json");
  });

  it("saves only the zip for a single-workflow selection", async () => {
    workflowPersistServiceSpy.retrieveWorkflow.mockReturnValue(of({ content: { op: "x" } } as any));

    await firstValueFrom(downloadService.downloadWorkflowsAsZip([{ id: 1, name: "Solo" }]));

    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledTimes(1);
    expect(fileSaverServiceSpy.saveAs.mock.calls[0][1]).toMatch(/^workflowExports-.*\.zip$/);
  });

  // The other direction: the standalone single-workflow download action must
  // keep saving, so the fix cannot simply drop the save from downloadWorkflow.
  it("still saves the file when a single workflow is downloaded on its own", async () => {
    workflowPersistServiceSpy.retrieveWorkflow.mockReturnValue(of({ content: { op: "x" } } as any));

    const item = await firstValueFrom(downloadService.downloadWorkflow(42, "MyWorkflow"));

    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledTimes(1);
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(item.blob, "MyWorkflow.json");
  });

  it("saves nothing when one of the workflows fails to retrieve", async () => {
    // The zip aborts as a whole, so the workflows that did come back must not be
    // left behind as loose files.
    workflowPersistServiceSpy.retrieveWorkflow.mockImplementation((id: number) =>
      id === 2 ? throwError(() => new Error("retrieve fail")) : (of({ content: { op: "x" } }) as any)
    );

    await expect(
      firstValueFrom(
        downloadService.downloadWorkflowsAsZip([
          { id: 1, name: "Alpha" },
          { id: 2, name: "Beta" },
          { id: 3, name: "Gamma" },
        ])
      )
    ).rejects.toThrow("retrieve fail");

    expect(fileSaverServiceSpy.saveAs).not.toHaveBeenCalled();
    expect(notificationServiceSpy.error).toHaveBeenCalledWith("Error downloading workflows as ZIP");
  });

  it("saves nothing for an empty selection", async () => {
    // The toolbar never calls this with an empty selection, but forkJoin([])
    // completes without emitting, so the chain must end without writing an
    // empty zip to disk.
    let completed = false;
    await new Promise<void>(resolve =>
      downloadService.downloadWorkflowsAsZip([]).subscribe({
        complete: () => {
          completed = true;
          resolve();
        },
      })
    );

    expect(completed).toBe(true);
    expect(fileSaverServiceSpy.saveAs).not.toHaveBeenCalled();
    expect(workflowPersistServiceSpy.retrieveWorkflow).not.toHaveBeenCalled();
  });

  it("writes the workflow content into the zip entries", async () => {
    workflowPersistServiceSpy.retrieveWorkflow.mockReturnValue(of({ content: { op: "x" } } as any));

    const result = await firstValueFrom(downloadService.downloadWorkflowsAsZip([{ id: 1, name: "Alpha" }]));

    // Reading the entry back proves the blob handed to JSZip is the serialized
    // workflow, not an empty or misrouted payload.
    const loaded = await JSZip.loadAsync(result);
    expect(JSON.parse(await loaded.files["Alpha.json"].async("string"))).toEqual({ op: "x" });
  });

  it("does not save anything when the standalone workflow download fails", async () => {
    workflowPersistServiceSpy.retrieveWorkflow.mockReturnValue(throwError(() => new Error("nope")));

    await expect(firstValueFrom(downloadService.downloadWorkflow(42, "MyWorkflow"))).rejects.toThrow("nope");

    expect(fileSaverServiceSpy.saveAs).not.toHaveBeenCalled();
  });

  // ─── downloadOperatorsResult ──────────────────────────────────────────────

  it("downloads a single operator file directly when there's exactly one file", async () => {
    const fileBlob = new Blob(["hello"], { type: "text/plain" });
    const result = await firstValueFrom(
      downloadService.downloadOperatorsResult([of([{ filename: "out.csv", blob: fileBlob }])], {
        wid: 1,
        name: "W",
      } as any)
    );

    expect(result).toBe(fileBlob);
    expect(notificationServiceSpy.info).toHaveBeenCalledWith("Starting to download operator result");
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(fileBlob, "out.csv");
    expect(notificationServiceSpy.success).toHaveBeenCalledWith("Operator result has been downloaded");
  });

  // The multi-file zip path goes through `new JSZip()`; now that the source
  // imports JSZip as a default import (rather than an import namespace object),
  // it constructs correctly under Vitest so this exercises the real archive.
  it("zips multiple operator files into a workflow-named archive", async () => {
    const a = new Blob(["a"], { type: "text/plain" });
    const b = new Blob(["b"], { type: "text/plain" });
    const result = await firstValueFrom(
      downloadService.downloadOperatorsResult(
        [
          of([
            { filename: "a.csv", blob: a },
            { filename: "b.csv", blob: b },
          ]),
        ],
        { wid: 7, name: "TwoFile" } as any
      )
    );

    expect(result).toBeInstanceOf(Blob);
    expect(notificationServiceSpy.info).toHaveBeenCalledWith("Starting to download operator results as ZIP");
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(expect.any(Blob), "results_7_TwoFile.zip");
    expect(notificationServiceSpy.success).toHaveBeenCalledWith("Operator results have been downloaded as ZIP");

    // The produced blob is a real zip carrying both operator files.
    const loaded = await JSZip.loadAsync(result);
    expect(Object.keys(loaded.files).sort()).toEqual(["a.csv", "b.csv"]);
  });

  it("flattens operator result batches before deciding single vs zip", async () => {
    // Two source observables, each emitting one file: flat() merges them into
    // two files, which routes through the multi-file zip branch.
    const result = await firstValueFrom(
      downloadService.downloadOperatorsResult(
        [
          of([{ filename: "first.csv", blob: new Blob(["1"], { type: "text/plain" }) }]),
          of([{ filename: "second.csv", blob: new Blob(["2"], { type: "text/plain" }) }]),
        ],
        { wid: 3, name: "Flat" } as any
      )
    );

    expect(result).toBeInstanceOf(Blob);
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledWith(expect.any(Blob), "results_3_Flat.zip");
    const loaded = await JSZip.loadAsync(result);
    expect(Object.keys(loaded.files).sort()).toEqual(["first.csv", "second.csv"]);
  });

  it("errors out cleanly when no operator result files are provided", async () => {
    await expect(
      firstValueFrom(downloadService.downloadOperatorsResult([of([])], { wid: 1, name: "Empty" } as any))
    ).rejects.toThrow("No files to download");
  });

  // ─── getWorkflowResultDownloadability ─────────────────────────────────────

  it("hits the downloadability endpoint and returns the operator → labels map", async () => {
    const promise = lastValueFrom(downloadService.getWorkflowResultDownloadability(99));
    const req = httpMock.expectOne(r => r.url.includes("/99/result/downloadability"));
    expect(req.request.method).toBe("GET");
    req.flush({ "op-1": ["my-dataset"], "op-2": [] });

    const map = await promise;
    expect(map).toEqual({ "op-1": ["my-dataset"], "op-2": [] });
  });

  // ─── exportWorkflowResultToDataset ────────────────────────────────────────

  it("POSTs the dataset export request and returns the response body", async () => {
    const promise = lastValueFrom(
      downloadService.exportWorkflowResultToDataset(
        "csv",
        1,
        "WF",
        EXPORT_OPERATORS,
        [7],
        0,
        0,
        "out.csv",
        computingUnit("local", 5)
      )
    );

    const req = httpMock.expectOne(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/${EXPORT_BASE_URL}/dataset`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toMatchObject({
      exportType: "csv",
      workflowId: 1,
      datasetIds: [7],
      filename: "out.csv",
      computingUnitId: 5,
    });
    expect(req.request.headers.get("Accept")).toBe("application/json");
    req.flush({ status: "ok", message: "done" });

    const res = await promise;
    expect(res.body).toEqual({ status: "ok", message: "done" });
  });

  it("appends the cuid query param for a kubernetes computing unit", () => {
    downloadService
      .exportWorkflowResultToDataset(
        "csv",
        1,
        "WF",
        EXPORT_OPERATORS,
        [7],
        0,
        0,
        "out.csv",
        computingUnit("kubernetes", 9)
      )
      .subscribe();

    const req = httpMock.expectOne(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/${EXPORT_BASE_URL}/dataset?cuid=9`);
    expect(req.request.method).toBe("POST");
    req.flush({ status: "ok", message: "done" });
  });

  // ─── exportWorkflowResultToLocal ──────────────────────────────────────────

  describe("exportWorkflowResultToLocal", () => {
    let submitSpy: ReturnType<typeof vi.spyOn>;
    let setTimeoutSpy: ReturnType<typeof vi.spyOn>;
    let cleanup: (() => void) | undefined;

    beforeEach(() => {
      // Stub form.submit (jsdom does not implement it) so we can assert it fired, and
      // capture the 10s cleanup callback instead of scheduling a real timer that could
      // fire during a later test (a leaked timer is flaky). Fake timers are avoided
      // because they make localStorage unavailable in this environment.
      submitSpy = vi.spyOn(HTMLFormElement.prototype, "submit").mockImplementation(() => {});
      cleanup = undefined;
      setTimeoutSpy = vi.spyOn(window, "setTimeout").mockImplementation((handler: TimerHandler) => {
        cleanup = handler as () => void;
        return 0 as unknown as ReturnType<typeof setTimeout>;
      });
      // localStorage is not available in this jsdom service-test environment; stub it so
      // the method can read the auth token deterministically.
      vi.stubGlobal("localStorage", {
        getItem: vi.fn().mockReturnValue("tok-123"),
        setItem: vi.fn(),
        removeItem: vi.fn(),
      });
    });

    afterEach(() => {
      submitSpy.mockRestore();
      setTimeoutSpy.mockRestore();
      vi.unstubAllGlobals();
      document
        .querySelectorAll('form[target="download-iframe"], iframe[name="download-iframe"]')
        .forEach(el => el.remove());
    });

    it("builds and submits a hidden form carrying the request and token, then cleans up on timeout", () => {
      downloadService.exportWorkflowResultToLocal(
        "csv",
        1,
        "WF",
        EXPORT_OPERATORS,
        0,
        0,
        "out.csv",
        computingUnit("local", 5)
      );

      const form = document.querySelector('form[target="download-iframe"]') as HTMLFormElement;
      expect(form).toBeTruthy();
      expect(form.getAttribute("action")).toBe(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/${EXPORT_BASE_URL}/local`);
      expect(form.method).toBe("post");
      expect(submitSpy).toHaveBeenCalledTimes(1);

      const requestInput = form.querySelector('input[name="request"]') as HTMLInputElement;
      expect(JSON.parse(requestInput.value)).toMatchObject({
        exportType: "csv",
        workflowId: 1,
        computingUnitId: 5,
        datasetIds: [],
      });
      expect((form.querySelector('input[name="token"]') as HTMLInputElement).value).toBe("tok-123");

      // Running the captured cleanup callback removes the form and the iframe.
      expect(document.querySelector('iframe[name="download-iframe"]')).toBeTruthy();
      cleanup?.();
      expect(document.querySelector('form[target="download-iframe"]')).toBeNull();
      expect(document.querySelector('iframe[name="download-iframe"]')).toBeNull();
    });

    it("falls back to an empty token when none is stored", () => {
      // Override the beforeEach stub so getItem returns null, exercising the
      // `?? ""` fallback on the auth token.
      vi.stubGlobal("localStorage", {
        getItem: vi.fn().mockReturnValue(null),
        setItem: vi.fn(),
        removeItem: vi.fn(),
      });

      downloadService.exportWorkflowResultToLocal(
        "csv",
        1,
        "WF",
        EXPORT_OPERATORS,
        0,
        0,
        "out.csv",
        computingUnit("local", 5)
      );

      const form = document.querySelector('form[target="download-iframe"]') as HTMLFormElement;
      expect((form.querySelector('input[name="token"]') as HTMLInputElement).value).toBe("");
    });

    it("targets the cuid-scoped endpoint for a kubernetes computing unit", () => {
      downloadService.exportWorkflowResultToLocal(
        "csv",
        1,
        "WF",
        EXPORT_OPERATORS,
        0,
        0,
        "out.csv",
        computingUnit("kubernetes", 9)
      );

      const form = document.querySelector('form[target="download-iframe"]') as HTMLFormElement;
      expect(form.getAttribute("action")).toBe(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/${EXPORT_BASE_URL}/local?cuid=9`);
    });
  });
});
