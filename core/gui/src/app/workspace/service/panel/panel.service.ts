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

import { Subject } from "rxjs";
import { Injectable } from "@angular/core";

@Injectable({
  providedIn: "root",
})
export class PanelService {
  private closePanelSubject = new Subject<void>();
  private resetPanelSubject = new Subject<void>();
  private openPropertyPanelSubject = new Subject<void>();
  private closePropertyPanelSubject = new Subject<void>();

  private _isPanelOpen = false;

  get isPanelOpen(): boolean {
    return this._isPanelOpen;
  }

  get resetPanelStream() {
    this._isPanelOpen = true;
    return this.resetPanelSubject.asObservable();
  }

  resetPanels() {
    this._isPanelOpen = true;
    this.resetPanelSubject.next();
  }

  get closePanelStream() {
    this._isPanelOpen = false;
    return this.closePanelSubject.asObservable();
  }

  closePanels() {
    this._isPanelOpen = false;
    this.closePanelSubject.next();
  }

  get openPropertyPanelStream() {
    this._isPanelOpen = true;
    return this.openPropertyPanelSubject.asObservable();
  }

  openPropertyPanel() {
    this._isPanelOpen = true;
    this.openPropertyPanelSubject.next();
  }

  get closePropertyPanelStream() {
    this._isPanelOpen = false;
    return this.closePropertyPanelSubject.asObservable();
  }

  closePropertyPanel() {
    this._isPanelOpen = false;
    this.closePropertyPanelSubject.next();
  }
}
