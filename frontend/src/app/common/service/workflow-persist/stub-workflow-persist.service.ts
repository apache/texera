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
import { DashboardWorkflow } from "../../../dashboard/type/dashboard-workflow.interface";
import { Workflow } from "../../type/workflow";
import { SearchFilterParameters, searchTestEntries } from "../../../dashboard/type/search-filter-parameters";
import { DashboardEntry } from "../../../dashboard/type/dashboard-entry";

export const WORKFLOW_BASE_URL = "workflow";
export const WORKFLOW_SEARCH_URL = WORKFLOW_BASE_URL + "/search";

@Injectable()
export class StubWorkflowPersistService {
  constructor(private testWorkflows: DashboardEntry[]) {}

  public retrieveWorkflow(wid: number): Observable<Workflow> {
    return new Observable(observer =>
      observer.next(this.testWorkflows.find(w => w.workflow.workflow.wid == wid)?.workflow.workflow)
    );
  }

  public searchWorkflows(keywords: string[], params: SearchFilterParameters): Observable<DashboardWorkflow[]> {
    return new Observable(observer => {
      return observer.next(searchTestEntries(keywords, params, this.testWorkflows, "workflow").map(i => i.workflow));
    });
  }
  /**
   * retrieves all workflow owners
   */
  public retrieveOwners(): Observable<string[]> {
    const names = this.testWorkflows.filter(i => i).map(i => i.workflow.ownerName) as string[];
    return new Observable(observer => {
      observer.next([...new Set(names)]);
    });
  }

  /**
   * retrieves all workflow IDs
   */
  public retrieveWorkflowIDs(): Observable<number[]> {
    return new Observable(observer => {
      observer.next(this.testWorkflows.map(i => i.workflow.workflow.wid as number).filter(i => i));
    });
  }
}
