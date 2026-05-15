/*
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

package org.apache.texera.workflow.macroOp

// Threaded through MacroExpander to detect macro recursion and depth bombs.
// `visited` is the set of (macroId, version) pairs on the current expansion path;
// reappearance means a cycle.
//
// Duplicate of the compiling-service equivalent; both versions track the same
// invariants because the amber-side and compiling-service-side WorkflowCompilers
// each maintain their own copy of the macro pipeline. They will converge when
// the broader LogicalPlan unification (see WorkflowCompiler.scala TODO) lands.
case class MacroCompileContext(
    visited: Set[(String, Int)],
    depth: Int
) {

  def guardAgainstCycle(macroId: String, version: Int): Unit = {
    if (visited.contains((macroId, version))) {
      val path = visited.map { case (id, v) => s"$id@v$v" }.mkString(" -> ")
      throw new IllegalStateException(
        s"Macro cycle detected: $macroId@v$version is already being expanded on this path " +
          s"(visited: $path)"
      )
    }
  }

  def guardAgainstDepth(): Unit = {
    if (depth >= MacroCompileContext.MaxDepth) {
      throw new IllegalStateException(
        s"Macro expansion depth limit (${MacroCompileContext.MaxDepth}) exceeded — " +
          s"likely a self-referential macro chain."
      )
    }
  }

  def descend(macroId: String, version: Int): MacroCompileContext =
    MacroCompileContext(visited + ((macroId, version)), depth + 1)
}

object MacroCompileContext {
  val MaxDepth: Int = 16
  def root: MacroCompileContext = MacroCompileContext(Set.empty, 0)
}
