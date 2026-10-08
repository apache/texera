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
import { Observable, Subject } from "rxjs";

/**
 * Which Loop Starts enclose each operator, as the latest editing-time compile reports it. The backend
 * works it out from the logical plan it compiles, and WorkflowCompilingService records here the
 * `operatorLoopStarts` of every compile response, successful or not.
 *
 * Two readers look again after every compile result: the property panel, which offers `$name`
 * loop-variable references only inside a loop block, and ValidationWorkflowService, which waives the
 * type check at such a reference only there. Every edit that can move an operator into or out of a block,
 * or change the variables a Loop Start declares, is followed by a compile, so neither needs to know which
 * edits those are.
 *
 * Until the first compile result, no operator counts as inside a loop block.
 *
 * A service of its own rather than part of WorkflowCompilingService because ValidationWorkflowService,
 * which WorkflowCompilingService injects, reads it.
 */
@Injectable({
  providedIn: "root",
})
export class LoopBlockService {
  private operatorLoopStarts: ReadonlyMap<string, ReadonlyArray<string>> = new Map();
  private readonly compileResultStream = new Subject<void>();

  /**
   * Records the `operatorLoopStarts` of a compile response, replacing the previous one, and announces it.
   * @param operatorLoopStarts for each operator inside a loop block, the Loop Starts of the blocks it is
   *                           inside; an operator outside every block is absent
   */
  public setOperatorLoopStarts(operatorLoopStarts: Readonly<Record<string, ReadonlyArray<string>>>): void {
    this.operatorLoopStarts = new Map(Object.entries(operatorLoopStarts));
    this.compileResultStream.next();
  }

  /**
   * The Loop Starts of the blocks the operator is inside, outermost first; empty for an operator outside
   * every block or not compiled yet. An outer block's Loop Start encloses an inner block's, so the fewer
   * Loop Starts enclose a Loop Start, the further out its block is; blocks that do not nest keep the
   * compile result's order.
   */
  public getEnclosingLoopStarts(operatorID: string): ReadonlyArray<string> {
    const depth = (loopStartID: string): number => this.operatorLoopStarts.get(loopStartID)?.length ?? 0;
    return [...(this.operatorLoopStarts.get(operatorID) ?? [])].sort((a, b) => depth(a) - depth(b));
  }

  /**
   * Emits after every compile result, including one that leaves every block as it was: the variables a
   * Loop Start declares may have changed all the same.
   */
  public getCompileResultStream(): Observable<void> {
    return this.compileResultStream.asObservable();
  }
}
