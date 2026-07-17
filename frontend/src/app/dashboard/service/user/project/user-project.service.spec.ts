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

import {
  CREATE_PROJECT_URL,
  DELETE_PROJECT_URL,
  USER_FILE_DELETE_URL,
  USER_PROJECT_BASE_URL,
  USER_PROJECT_LIST_URL,
  UserProjectService,
} from "./user-project.service";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { TestBed } from "@angular/core/testing";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { DashboardProject } from "../../../type/dashboard-project.interface";
import { DashboardWorkflow } from "../../../type/dashboard-workflow.interface";
import { DashboardFile } from "../../../type/dashboard-file.interface";

const mockProject: DashboardProject = {
  pid: 1,
  name: "proj",
  description: "desc",
  ownerId: 7,
  creationTime: 0,
  color: null,
  accessLevel: "WRITE",
};

const mockFile: DashboardFile = {
  ownerEmail: "owner@test.com",
  accessLevel: "READ",
  file: { ownerUid: 7, fid: 3, size: 10, name: "data.csv", path: "/data.csv", description: "", uploadTime: 0 },
};

const mockWorkflows = [{ projectIDs: [1], accessLevel: "READ" }] as unknown as DashboardWorkflow[];
const mockResponse = { ok: true } as unknown as Response;

describe("UserProjectService", () => {
  let service: UserProjectService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      // UserProjectService never calls NotificationService; stub it so TestBed
      // does not have to construct the ng-zorro message/notification services.
      providers: [UserProjectService, { provide: NotificationService, useValue: {} }],
    });
    service = TestBed.inject(UserProjectService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("getProjectList issues GET to the list url", () => {
    let result: DashboardProject[] | undefined;
    service.getProjectList().subscribe(res => (result = res));
    const req = httpMock.expectOne(USER_PROJECT_LIST_URL);
    expect(req.request.method).toBe("GET");
    req.flush([mockProject]);
    expect(result).toEqual([mockProject]);
  });

  it("retrieveWorkflowsOfProject issues GET to the project workflows url", () => {
    let result: DashboardWorkflow[] | undefined;
    service.retrieveWorkflowsOfProject(1).subscribe(res => (result = res));
    const req = httpMock.expectOne(`${USER_PROJECT_BASE_URL}/1/workflows`);
    expect(req.request.method).toBe("GET");
    req.flush(mockWorkflows);
    expect(result).toEqual(mockWorkflows);
  });

  it("retrieveFilesOfProject issues GET to the project files url", () => {
    let result: DashboardFile[] | undefined;
    service.retrieveFilesOfProject(1).subscribe(res => (result = res));
    const req = httpMock.expectOne(`${USER_PROJECT_BASE_URL}/1/files`);
    expect(req.request.method).toBe("GET");
    req.flush([mockFile]);
    expect(result).toEqual([mockFile]);
  });

  it("retrieveProject issues GET to the project url", () => {
    let result: DashboardProject | undefined;
    service.retrieveProject(1).subscribe(res => (result = res));
    const req = httpMock.expectOne(`${USER_PROJECT_BASE_URL}/1`);
    expect(req.request.method).toBe("GET");
    req.flush(mockProject);
    expect(result).toEqual(mockProject);
  });

  it("createProject issues POST to the create url with the name in the path", () => {
    let result: DashboardProject | undefined;
    service.createProject("proj").subscribe(res => (result = res));
    const req = httpMock.expectOne(`${CREATE_PROJECT_URL}/proj`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({});
    req.flush(mockProject);
    expect(result).toEqual(mockProject);
  });

  it("updateProjectName issues POST to the rename url", () => {
    service.updateProjectName(1, "renamed").subscribe();
    const req = httpMock.expectOne(`${USER_PROJECT_BASE_URL}/1/rename/renamed`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({});
    req.flush(mockResponse);
  });

  it("updateProjectDescription issues POST with the description as the raw body", () => {
    service.updateProjectDescription(1, "a new description").subscribe();
    const req = httpMock.expectOne(`${USER_PROJECT_BASE_URL}/1/update/description`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toBe("a new description");
    req.flush(mockResponse);
  });

  it("deleteProject issues DELETE to the delete url", () => {
    service.deleteProject(1).subscribe();
    const req = httpMock.expectOne(`${DELETE_PROJECT_URL}/1`);
    expect(req.request.method).toBe("DELETE");
    req.flush(mockResponse);
  });

  it("addWorkflowToProject issues POST to the add-workflow url", () => {
    service.addWorkflowToProject(1, 2).subscribe();
    const req = httpMock.expectOne(`${USER_PROJECT_BASE_URL}/1/workflow/2/add`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({});
    req.flush(mockResponse);
  });

  it("removeWorkflowFromProject issues DELETE to the remove-workflow url", () => {
    service.removeWorkflowFromProject(1, 2).subscribe();
    const req = httpMock.expectOne(`${USER_PROJECT_BASE_URL}/1/workflow/2/delete`);
    expect(req.request.method).toBe("DELETE");
    req.flush(mockResponse);
  });

  it("addFileToProject issues POST to the add-file url", () => {
    service.addFileToProject(1, 3).subscribe();
    const req = httpMock.expectOne(`${USER_PROJECT_BASE_URL}/1/user-file/3/add`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({});
    req.flush(mockResponse);
  });

  it("updateProjectColor issues POST to the color-add url", () => {
    service.updateProjectColor(1, "ff0000").subscribe();
    const req = httpMock.expectOne(`${USER_PROJECT_BASE_URL}/1/color/ff0000/add`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({});
    req.flush(mockResponse);
  });

  it("deleteProjectColor issues POST to the color-delete url", () => {
    service.deleteProjectColor(1).subscribe();
    const req = httpMock.expectOne(`${USER_PROJECT_BASE_URL}/1/color/delete`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({});
    req.flush(mockResponse);
  });

  it("removeFileFromProject issues DELETE to the remove-file url", () => {
    service.removeFileFromProject(1, 3).subscribe();
    const req = httpMock.expectOne(`${USER_PROJECT_BASE_URL}/1/user-file/3/delete`);
    expect(req.request.method).toBe("DELETE");
    req.flush(mockResponse);
  });

  it("getProjectFiles returns an empty list before any fetch", () => {
    expect(service.getProjectFiles()).toEqual([]);
  });

  it("refreshFilesOfProject fetches files and caches them for getProjectFiles", () => {
    service.refreshFilesOfProject(1);
    const req = httpMock.expectOne(`${USER_PROJECT_BASE_URL}/1/files`);
    expect(req.request.method).toBe("GET");
    req.flush([mockFile]);
    expect(service.getProjectFiles()).toEqual([mockFile]);
  });

  it("deleteDashboardUserFileEntry deletes the file then refreshes the cached project files", () => {
    service.deleteDashboardUserFileEntry(1, mockFile);

    const deleteReq = httpMock.expectOne(`${USER_FILE_DELETE_URL}/${mockFile.file.name}/${mockFile.ownerEmail}`);
    expect(deleteReq.request.method).toBe("DELETE");
    deleteReq.flush({});

    const refreshReq = httpMock.expectOne(`${USER_PROJECT_BASE_URL}/1/files`);
    expect(refreshReq.request.method).toBe("GET");
    refreshReq.flush([mockFile]);

    expect(service.getProjectFiles()).toEqual([mockFile]);
  });
});

describe("UserProjectService color helpers", () => {
  it("isInvalidColorFormat flags null, wrong-length, and non-hex strings", () => {
    expect(UserProjectService.isInvalidColorFormat(null as unknown as string)).toBe(true);
    expect(UserProjectService.isInvalidColorFormat("ff")).toBe(true); // too short
    expect(UserProjectService.isInvalidColorFormat("ffff")).toBe(true); // length 4
    expect(UserProjectService.isInvalidColorFormat("gggggg")).toBe(true); // non-hex
    expect(UserProjectService.isInvalidColorFormat("fff")).toBe(false); // 3-digit hex
    expect(UserProjectService.isInvalidColorFormat("ffffff")).toBe(false); // 6-digit hex
    expect(UserProjectService.isInvalidColorFormat("1A2b3C")).toBe(false); // mixed case
  });

  it("isLightColor is true for light colors and false for dark or invalid ones", () => {
    expect(UserProjectService.isLightColor("ffffff")).toBe(true); // white
    expect(UserProjectService.isLightColor("fff")).toBe(true); // 3-digit white expands to ffffff
    expect(UserProjectService.isLightColor("000000")).toBe(false); // black
    expect(UserProjectService.isLightColor("zz")).toBe(false); // invalid format -> dark default
  });
});
