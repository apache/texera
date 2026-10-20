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

import { HttpClient, HttpHeaders } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { LogicalPlan } from "../../../../workspace/types/execute-workflow.interface";

export const WORKFLOW_TO_PYTHON_ENDPOINT = "workflow-to-python";

export interface WorkflowToPythonResponse {
  type: "success" | "failure";
  pythonCode?: string;
  errorMessage?: string;
}

@Injectable({
  providedIn: "root",
})
export class WorkflowToPythonService {
  constructor(private httpClient: HttpClient) {}

  public convertToPython(logicalPlan: LogicalPlan): Observable<WorkflowToPythonResponse> {
    const body = {
      operators: logicalPlan.operators,
      links: logicalPlan.links,
      opsToReuseResult: [],
      opsToViewResult: [],
    };

    return this.httpClient.post<WorkflowToPythonResponse>(
      `${AppSettings.getApiEndpoint()}/${WORKFLOW_TO_PYTHON_ENDPOINT}`,
      body,
      {
        headers: new HttpHeaders({
          "Content-Type": "application/json",
        }),
      }
    );
  }
}
