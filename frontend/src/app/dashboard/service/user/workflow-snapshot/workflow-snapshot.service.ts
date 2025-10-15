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

import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { HttpClient } from "@angular/common/http";
import html2canvas from "html2canvas";
import { WorkflowSnapshotEntry } from "../../../type/workflow-snapshot-entry";

export const WORKFLOW_SNAPSHOT_API_BASE_URL = `${AppSettings.getApiEndpoint()}/snapshot`;
export const WORKFLOW_SNAPSHOT_UPLOAD_URL = `${WORKFLOW_SNAPSHOT_API_BASE_URL}/upload`;

@Injectable({
  providedIn: "root",
})
export class WorkflowSnapshotService {
  constructor(private http: HttpClient) {}

  /**
   * create canvas for snapshot
   */
  public createSnapShotCanvas(
    heightRatio: number,
    yRatio: number,
    widthRatio: number,
    xRatio: number
  ): Promise<HTMLCanvasElement> {
    let doc = document.getElementById("texera-workflow-editor") || document.body;
    const { height, width } = doc.getBoundingClientRect();
    return html2canvas(doc, {
      allowTaint: true,
      useCORS: true,
      backgroundColor: "transparent",
      height: height * heightRatio,
      y: height * yRatio,
      width: width * widthRatio,
      x: width * xRatio,
    });
  }

  /**
   * store snapshot into sql
   */
  public uploadWorkflowSnapshot(snapshotBlob: Blob, wid: number | undefined): Observable<Response> {
    const formData: FormData = new FormData();
    formData.append("wid", wid?.toString() || "");
    formData.append("SnapshotBlob", snapshotBlob);
    return this.http.put<Response>(`${WORKFLOW_SNAPSHOT_UPLOAD_URL}`, formData);
  }

  /**
   * retrieve the snapshot
   */
  public retrieveWorkflowSnapshot(sid: number): Observable<WorkflowSnapshotEntry> {
    return this.http.get<WorkflowSnapshotEntry>(`${WORKFLOW_SNAPSHOT_API_BASE_URL}/${sid}`);
  }
}
