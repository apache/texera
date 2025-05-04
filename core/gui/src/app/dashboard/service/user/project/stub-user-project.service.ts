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

import { Observable } from "rxjs";
import { DashboardProject } from "../../../type/dashboard-project.interface";
import { DashboardWorkflow } from "../../../type/dashboard-workflow.interface";
import { DashboardFile } from "../../../type/dashboard-file.interface";
import { UserProjectService } from "./user-project.service";
import { testUserProjects } from "../../../component/user-dashboard-test-fixtures";

export class StubUserProjectService {
  public getProjectList(): Observable<DashboardProject[]> {
    return new Observable(observer => observer.next(testUserProjects.slice()));
  }

  public retrieveWorkflowsOfProject(pid: number): Observable<DashboardWorkflow[]> {
    throw new Error("Not implemented.");
  }

  public retrieveFilesOfProject(pid: number): Observable<DashboardFile[]> {
    throw new Error("Not implemented.");
  }

  public getProjectFiles(): ReadonlyArray<DashboardFile> {
    throw new Error("Not implemented.");
  }

  public refreshFilesOfProject(pid: number): void {
    throw new Error("Not implemented.");
  }

  public retrieveProject(pid: number): Observable<DashboardProject> {
    throw new Error("Not implemented.");
  }

  public updateProjectName(pid: number, name: string): Observable<Response> {
    throw new Error("Not implemented.");
  }

  public updateProjectDescription(pid: number, description: string): Observable<Response> {
    throw new Error("Not implemented.");
  }

  public deleteProject(pid: number): Observable<Response> {
    throw new Error("Not implemented.");
  }

  public createProject(name: string): Observable<DashboardProject> {
    throw new Error("Not implemented.");
  }

  public addWorkflowToProject(pid: number, wid: number): Observable<Response> {
    throw new Error("Not implemented.");
  }

  public removeWorkflowFromProject(pid: number, wid: number): Observable<Response> {
    throw new Error("Not implemented.");
  }

  public addFileToProject(pid: number, fid: number): Observable<Response> {
    throw new Error("Not implemented.");
  }

  public updateProjectColor(pid: number, colorHex: string): Observable<Response> {
    throw new Error("Not implemented.");
  }

  public deleteProjectColor(pid: number): Observable<Response> {
    throw new Error("Not implemented.");
  }

  public removeFileFromProject(pid: number, fid: number): Observable<Response> {
    throw new Error("Not implemented.");
  }

  /**
   * same as UserFileService"s deleteDashboardUserFileEntry method, except
   * it is modified to refresh the project"s list of files
   */
  public deleteDashboardUserFileEntry(pid: number, targetUserFileEntry: DashboardFile): void {
    throw new Error("Not implemented.");
  }

  /**
   * Helper function to determine if a project color is light
   * or dark, which can be helpful for styling decisions
   *
   * @param color (HEX formatted color string)
   * @returns boolean indicating whether color is "light" or "dark"
   */
  public isLightColor(color: string): boolean {
    return UserProjectService.isLightColor(color);
  }

  /**
   * Helper function to validate if a project color is in HEX format
   *
   * @param color
   * @returns boolean indicating whether color is in valid HEX format
   */
  public isInvalidColorFormat(color: string): boolean {
    return UserProjectService.isInvalidColorFormat(color);
  }
}
