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

/**
 * Coordinates parallel tool calls that create interdependent operators.
 *
 * When the LLM issues multiple createOrModifyOperator calls simultaneously
 * (via Promise.all in the Vercel AI SDK), a dependent call like
 * `def process(A)` would fail because operator A hasn't been added yet.
 *
 * This coordinator lets dependent calls wait for their sibling calls to
 * finish instead of failing immediately. Independent calls run in full
 * parallel; only dependent calls block on their specific dependencies.
 *
 * Usage inside a tool's execute function:
 *   register(id)               — sync, before first await
 *   waitForDependencies(deps)  — async, before input validation
 *   markDone(id)               — in finally block
 */
export class ParallelCallCoordinator {
  private pending = new Map<string, { promise: Promise<void>; resolve: () => void }>();
  private readonly timeoutMs: number;

  constructor(timeoutMs = 60000) {
    this.timeoutMs = timeoutMs;
  }

  /**
   * Declare that this operatorId is being created by the current call.
   * Must be called synchronously (before any await) so all parallel calls
   * register before any of them start waiting.
   */
  register(operatorId: string): void {
    if (this.pending.has(operatorId)) return;
    let resolve!: () => void;
    const promise = new Promise<void>(r => {
      resolve = r;
    });
    this.pending.set(operatorId, { promise, resolve });
  }

  /**
   * Wait for dependencies that are being created by sibling parallel calls.
   *
   * - Dependency already in workflow → skip.
   * - Dependency registered here     → await its completion.
   * - Dependency unknown              → skip (normal validation will error).
   */
  async waitForDependencies(
    dependencies: string[],
    operatorExists: (id: string) => boolean
  ): Promise<void> {
    for (const dep of dependencies) {
      if (operatorExists(dep)) continue;

      const entry = this.pending.get(dep);
      if (!entry) continue;

      await Promise.race([
        entry.promise,
        new Promise<void>((_, reject) =>
          setTimeout(() => reject(new Error(`Timeout waiting for parallel operator "${dep}"`)), this.timeoutMs)
        ),
      ]);
    }
  }

  /**
   * Signal that the operator has been created (or the call has finished).
   * Unblocks any sibling calls waiting on this operator.
   */
  markDone(operatorId: string): void {
    const entry = this.pending.get(operatorId);
    if (entry) {
      entry.resolve();
      this.pending.delete(operatorId);
    }
  }
}
