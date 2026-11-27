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

import { Injectable, ViewContainerRef } from "@angular/core";
import { BehaviorSubject, Observable } from "rxjs";

@Injectable({
  providedIn: "root",
})
export class CodeEditorService {
  /**
   * Where the code editor renders: the outlet of the view in front, marked `texeraCodeEditorHost`
   * on an `<ng-template>` in its template (see CodeEditorHostDirective). The operator canvas and the
   * Form View each carry one; a view's outlet is attached when its template is built and detached
   * when the view is destroyed. The editor is a floating box, so where the outlet sits in the page
   * does not matter -- only that the view in front has one.
   */
  private editorHost?: ViewContainerRef;

  /** The outlet to open the editor into, while a view hosts one. */
  get host(): ViewContainerRef | undefined {
    return this.editorHost;
  }

  attachHost(host: ViewContainerRef): void {
    this.editorHost = host;
  }

  /** Lets go of this host only: a host that has since replaced it stays attached. */
  detachHost(host: ViewContainerRef): void {
    if (this.editorHost === host) {
      this.editorHost = undefined;
    }
  }

  private editorStates: Map<string, BehaviorSubject<boolean>> = new Map();

  /**
   * Returns an observable representing whether the editor for the given operator is open.
   * @param operatorID The ID of the operator.
   * @returns Observable for the editor state.
   */
  getEditorState(operatorID: string): Observable<boolean> {
    if (!this.editorStates.has(operatorID)) {
      this.editorStates.set(operatorID, new BehaviorSubject<boolean>(false));
    }
    return this.editorStates.get(operatorID)!.asObservable();
  }

  /**
   * Sets the editor state for the given operator.
   * @param operatorID The ID of the operator.
   * @param isOpen Whether the editor is open.
   */
  setEditorState(operatorID: string, isOpen: boolean): void {
    if (!this.editorStates.has(operatorID)) {
      this.editorStates.set(operatorID, new BehaviorSubject<boolean>(isOpen));
    } else {
      this.editorStates.get(operatorID)!.next(isOpen);
    }
  }
}
