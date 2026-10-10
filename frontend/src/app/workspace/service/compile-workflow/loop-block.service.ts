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
 * The compile is sent only the operators ValidationWorkflowService judges valid, so while one is left out,
 * as an operator in a block is while it holds "$" on the way to "$K", a compile result misses the blocks
 * around that operator, and around the ones whose path to their Loop End runs through it. Such a result
 * does not take an operator out of a block; see setOperatorLoopStarts.
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
   * Records the `operatorLoopStarts` of a compile response and announces it.
   *
   * A compile of the whole workflow replaces the previous result. What a compile that was not sent every
   * operator reports still holds, since each path it sees is one in the workflow and one path is enough to
   * put an operator inside a block; but an operator it finds outside every block may not be. So each
   * operator also keeps the blocks the previous result put it in, until a compile of the whole workflow.
   * @param operatorLoopStarts for each operator inside a loop block, the Loop Starts of the blocks it is
   *                           inside; an operator outside every block is absent
   * @param wholeWorkflow whether the compile was sent every operator of the workflow
   */
  public setOperatorLoopStarts(
    operatorLoopStarts: Readonly<Record<string, ReadonlyArray<string>>>,
    wholeWorkflow = true
  ): void {
    const reported = new Map(Object.entries(operatorLoopStarts));
    if (!wholeWorkflow) {
      this.operatorLoopStarts.forEach((loopStarts, operatorID) =>
        reported.set(operatorID, [...new Set([...(reported.get(operatorID) ?? []), ...loopStarts])])
      );
    }
    this.operatorLoopStarts = reported;
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
