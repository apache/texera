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
import { BehaviorSubject, Observable, Subject } from "rxjs";

@Injectable({ providedIn: "root" })
export class AgentPanelControlService {
  private readonly toggleRequestSubject = new Subject<void>();
  private readonly openStateSubject = new BehaviorSubject<boolean>(false);

  public readonly toggleRequest$: Observable<void> = this.toggleRequestSubject.asObservable();
  /** Current open/closed state of the AI agent panel, kept in sync by the panel itself. */
  public readonly openState$: Observable<boolean> = this.openStateSubject.asObservable();

  public requestToggle(): void {
    this.toggleRequestSubject.next();
  }

  public setOpenState(isOpen: boolean): void {
    if (this.openStateSubject.value !== isOpen) {
      this.openStateSubject.next(isOpen);
    }
  }
}
