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

package org.apache.texera.amber.compiler.macroOp

import org.apache.texera.amber.operator.macroOp.MacroBody

// Looks up a macro definition's body by (macroId, version). The persistence-backed
// implementation lives in the amber service and queries workflow_version; tests and
// services without persistence can use Empty or inMemory.
trait MacroRegistry {
  def fetch(macroId: String, version: Int): Option[MacroBody]
}

object MacroRegistry {

  // Always returns None. Use when persistence is not wired up — SNAPSHOT macros still
  // work since their body is embedded; LIVE macros fail with "not found in registry".
  object Empty extends MacroRegistry {
    override def fetch(macroId: String, version: Int): Option[MacroBody] = None
  }

  // For tests: a fixed table of bodies keyed by (id, version).
  def inMemory(bodies: Map[(String, Int), MacroBody]): MacroRegistry =
    new MacroRegistry {
      override def fetch(macroId: String, version: Int): Option[MacroBody] =
        bodies.get((macroId, version))
    }
}
