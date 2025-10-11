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
  public vc!: ViewContainerRef;

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
