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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Injectable } from "@angular/core";
import { Observable, of } from "rxjs";
import { DashboardModel } from "../../../type/dashboard-model.interface";

/**
 * In-memory ModelService double, so specs need no HttpTestingController.
 *
 * Lives next to the real service per the frontend placement rules, and is shared by
 * every spec that renders a Models view rather than each writing its own partial mock.
 */
@Injectable()
export class StubModelService {
  constructor(private models: DashboardModel[] = []) {}

  public retrieveAccessibleModels(): Observable<DashboardModel[]> {
    return of(this.models);
  }

  public deleteModel(mid: number): Observable<Response> {
    this.models = this.models.filter(model => model.model.mid !== mid);
    return of({} as Response);
  }

  public retrieveOwners(): Observable<string[]> {
    return of([...new Set(this.models.map(model => model.ownerEmail))]);
  }

  public updateModelName(): Observable<Response> {
    return of({} as Response);
  }

  public updateModelDescription(): Observable<Response> {
    return of({} as Response);
  }

  public updateModelPublicity(): Observable<Response> {
    return of({} as Response);
  }

  public updateModelDownloadable(): Observable<Response> {
    return of({} as Response);
  }
}
