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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { of } from "rxjs";
import { ResourceRegistryService } from "./resource-registry.service";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { EntityType } from "../../../../hub/service/hub.service";
import { DatasetService } from "../dataset/dataset.service";
import { ModelService } from "../model/model.service";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { DownloadService } from "../download/download.service";
import {
  HUB_DATASET_RESULT_DETAIL,
  HUB_WORKFLOW_RESULT_DETAIL,
  USER_DATASET,
  USER_MODEL,
  USER_WORKSPACE,
} from "../../../../app-routing.constant";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { MODEL_ICON } from "../../../../common/icon/model-icon";

const entry = (overrides: Partial<Record<string, unknown>>): DashboardEntry =>
  ({ id: 7, accessibleUserIds: [], ...overrides }) as unknown as DashboardEntry;

describe("ResourceRegistryService", () => {
  let registry: ResourceRegistryService;
  let workflowPersistService: { [k: string]: ReturnType<typeof vi.fn> };
  let datasetService: { [k: string]: ReturnType<typeof vi.fn> };
  let modelService: { [k: string]: ReturnType<typeof vi.fn> };
  let downloadService: { [k: string]: ReturnType<typeof vi.fn> };

  beforeEach(() => {
    // Partial spies on purpose: the descriptors must not touch these until a caller asks.
    workflowPersistService = {
      updateWorkflowName: vi.fn().mockReturnValue(of({})),
      updateWorkflowDescription: vi.fn().mockReturnValue(of({})),
      retrieveOwners: vi.fn().mockReturnValue(of(["wf-owner"])),
      retrieveWorkflowIDs: vi.fn().mockReturnValue(of([1, 2])),
    };
    datasetService = {
      updateDatasetName: vi.fn().mockReturnValue(of({})),
      updateDatasetDescription: vi.fn().mockReturnValue(of({})),
      retrieveOwners: vi.fn().mockReturnValue(of(["ds-owner"])),
      retrieveDatasetVersionSingleFile: vi.fn().mockReturnValue(of(new Blob())),
    };

    modelService = {
      updateModelName: vi.fn().mockReturnValue(of({})),
      updateModelDescription: vi.fn().mockReturnValue(of({})),
      retrieveModelVersionSingleFile: vi.fn().mockReturnValue(of(new Blob())),
    };

    downloadService = {
      downloadWorkflow: vi.fn().mockReturnValue(of({})),
      downloadDataset: vi.fn().mockReturnValue(of(new Blob())),
      downloadModel: vi.fn().mockReturnValue(of(new Blob())),
    };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        { provide: DownloadService, useValue: downloadService },
        { provide: WorkflowPersistService, useValue: workflowPersistService },
        { provide: DatasetService, useValue: datasetService },
        { provide: ModelService, useValue: modelService },
        ...commonTestProviders,
      ],
    });
    registry = TestBed.inject(ResourceRegistryService);
  });

  // ─── lookup ───────────────────────────────────────────────────────────────

  it("resolves every kind the dashboard renders", () => {
    expect(registry.get(EntityType.Workflow).iconType).toBe("project");
    expect(registry.get(EntityType.Dataset).iconType).toBe("database");
    expect(registry.get(EntityType.File).iconType).toBe("folder-open");
    expect(registry.get(EntityType.Model).iconType).toBe(MODEL_ICON);
  });

  it("refuses a kind it does not carry", () => {
    // Computing units reach the dashboard as entities but never as list entries.
    expect(() => registry.get(EntityType.ComputingUnit)).toThrowError("Unexpected type in DashboardEntry.");
    expect(() => registry.get("quantum" as EntityType)).toThrowError("Unexpected type in DashboardEntry.");
  });

  // ─── capability checks ────────────────────────────────────────────────────

  it("exposes rename and description only for the kinds that support them", () => {
    for (const type of [EntityType.Workflow, EntityType.Dataset, EntityType.Model]) {
      expect(registry.get(type).rename).toBeDefined();
      expect(registry.get(type).updateDescription).toBeDefined();
    }
    // Models gain `retrieveOwners` with the share modal and the filters, which are the only callers.
    expect(registry.get(EntityType.Model).retrieveOwners).toBeUndefined();
    for (const type of [EntityType.File]) {
      expect(registry.get(type).rename).toBeUndefined();
      expect(registry.get(type).updateDescription).toBeUndefined();
    }
  });

  it("offers a download and a file preview only for the kinds that hold files", () => {
    for (const type of [EntityType.Workflow, EntityType.Dataset, EntityType.Model]) {
      expect(registry.get(type).download).toBeDefined();
    }
    for (const type of [EntityType.File]) {
      expect(registry.get(type).download).toBeUndefined();
    }
    // Workflows are one file, not a version tree, so nothing previews them.
    expect(registry.get(EntityType.Workflow).retrieveSingleFile).toBeUndefined();
    expect(registry.get(EntityType.Dataset).retrieveSingleFile).toBeDefined();
    expect(registry.get(EntityType.Model).retrieveSingleFile).toBeDefined();
  });

  it("offers an id filter only where the backend has an id endpoint", () => {
    expect(registry.get(EntityType.Workflow).retrieveIds).toBeDefined();
    expect(registry.get(EntityType.Dataset).retrieveIds).toBeUndefined();
    expect(registry.get(EntityType.Model).retrieveIds).toBeUndefined();
  });

  it("validates names only where the backend restricts them", () => {
    expect(registry.get(EntityType.Workflow).validateName).toBeUndefined();
    expect(registry.get(EntityType.Dataset).validateName!("has spaces")).toContain("Invalid dataset name");
    expect(registry.get(EntityType.Dataset).validateName!("fine-name_1")).toBeNull();
    expect(registry.get(EntityType.Model).validateName!("has spaces")).toContain("Invalid model name");
    expect(registry.get(EntityType.Model).validateName!("resnet-50")).toBeNull();
  });

  // ─── delegation ───────────────────────────────────────────────────────────

  it("delegates each operation to the owning service", () => {
    registry.get(EntityType.Workflow).rename!(1, "wf");
    registry.get(EntityType.Workflow).updateDescription!(1, "d");
    registry.get(EntityType.Dataset).rename!(2, "ds");
    registry.get(EntityType.Dataset).updateDescription!(2, "d");
    registry.get(EntityType.Model).rename!(3, "m");
    registry.get(EntityType.Model).updateDescription!(3, "d");
    registry.get(EntityType.Workflow).download!(1, "wf");
    registry.get(EntityType.Dataset).download!(2, "ds");
    registry.get(EntityType.Model).download!(3, "m");
    registry.get(EntityType.Dataset).retrieveSingleFile!("/dataset/a/ds/v1/f.csv", true);
    registry.get(EntityType.Model).retrieveSingleFile!("/model/a/m/v1/f.pt", false);

    expect(workflowPersistService["updateWorkflowName"]).toHaveBeenCalledWith(1, "wf");
    expect(workflowPersistService["updateWorkflowDescription"]).toHaveBeenCalledWith(1, "d");
    expect(datasetService["updateDatasetName"]).toHaveBeenCalledWith(2, "ds");
    expect(datasetService["updateDatasetDescription"]).toHaveBeenCalledWith(2, "d");
    expect(modelService["updateModelName"]).toHaveBeenCalledWith(3, "m");
    expect(modelService["updateModelDescription"]).toHaveBeenCalledWith(3, "d");
    expect(downloadService["downloadWorkflow"]).toHaveBeenCalledWith(1, "wf");
    expect(downloadService["downloadDataset"]).toHaveBeenCalledWith(2, "ds");
    expect(downloadService["downloadModel"]).toHaveBeenCalledWith(3, "m");
    expect(datasetService["retrieveDatasetVersionSingleFile"]).toHaveBeenCalledWith("/dataset/a/ds/v1/f.csv", true);
    expect(modelService["retrieveModelVersionSingleFile"]).toHaveBeenCalledWith("/model/a/m/v1/f.pt", false);
  });

  it("reads ownership off the kind's own payload", () => {
    expect(registry.get(EntityType.Workflow).isOwner(entry({ workflow: { isOwner: false } }))).toBe(false);
    expect(registry.get(EntityType.Dataset).isOwner(entry({ dataset: { isOwner: true } }))).toBe(true);
    expect(registry.get(EntityType.Model).isOwner(entry({ model: { isOwner: false } }))).toBe(false);
    // File entries carry no ownership payload at all, so theirs must not read one.
    expect(registry.get(EntityType.File).isOwner(entry({}))).toBe(true);
  });

  // ─── entryLink ────────────────────────────────────────────────────────────

  it("routes a viewer with access to the private page and everyone else to the hub", () => {
    const workflow = entry({ type: EntityType.Workflow, id: 7, accessibleUserIds: [42] });
    expect(registry.entryLink(workflow, 42)).toEqual([USER_WORKSPACE, "7"]);
    expect(registry.entryLink(workflow, 99)).toEqual([HUB_WORKFLOW_RESULT_DETAIL, "7"]);
    // An anonymous viewer is not on anyone's access list.
    expect(registry.entryLink(workflow, undefined)).toEqual([HUB_WORKFLOW_RESULT_DETAIL, "7"]);

    const dataset = entry({ type: EntityType.Dataset, id: 5, accessibleUserIds: [42] });
    expect(registry.entryLink(dataset, 42)).toEqual([USER_DATASET, "5"]);
    expect(registry.entryLink(dataset, 99)).toEqual([HUB_DATASET_RESULT_DETAIL, "5"]);
  });

  it("routes a model to its detail page, which has no hub twin yet", () => {
    expect(registry.get(EntityType.Model).hubRoute).toBeUndefined();
    expect(registry.entryLink(entry({ type: EntityType.Model, id: 9 }), 42)).toEqual([USER_MODEL, "9"]);
  });

  it("leaves an unroutable or unsaved entry unlinked", () => {
    expect(registry.entryLink(entry({ type: EntityType.File, id: 8 }), 42)).toEqual([]);
    expect(registry.entryLink(entry({ type: EntityType.Dataset, id: undefined }), 42)).toEqual([]);
    expect(registry.entryLink(entry({ type: EntityType.Workflow, id: "draft" }), 42)).toEqual([]);
  });
});
