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

package org.apache.texera.amber.core.storage.result

import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.scalatest.flatspec.AnyFlatSpec

class WorkflowResultStoreSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  private val opA = OperatorIdentity("op-a")
  private val opB = OperatorIdentity("op-b")

  // ---------------------------------------------------------------------------
  // Default state
  // ---------------------------------------------------------------------------

  "WorkflowResultStore()" should "default resultInfo to Map.empty" in {
    assert(WorkflowResultStore().resultInfo.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // Custom map preserves entries
  // ---------------------------------------------------------------------------

  "WorkflowResultStore(...)" should
    "preserve a Map carrying metadata entries for multiple operators" in {
    val store = WorkflowResultStore(
      Map(
        opA -> OperatorResultMetadata(10, "h-a"),
        opB -> OperatorResultMetadata(20, "h-b")
      )
    )
    assert(store.resultInfo.size == 2)
    assert(store.resultInfo(opA) == OperatorResultMetadata(10, "h-a"))
    assert(store.resultInfo(opB) == OperatorResultMetadata(20, "h-b"))
  }

  it should "preserve key identity (a missing key reads as None via .get)" in {
    val store = WorkflowResultStore(Map(opA -> OperatorResultMetadata(1, "x")))
    assert(store.resultInfo.get(opA).contains(OperatorResultMetadata(1, "x")))
    assert(store.resultInfo.get(opB).isEmpty)
  }

  // ---------------------------------------------------------------------------
  // Equality / hashCode (case class semantics)
  // ---------------------------------------------------------------------------

  "WorkflowResultStore equality" should
    "compare resultInfo by value (two stores with the same Map are equal)" in {
    val s1 = WorkflowResultStore(Map(opA -> OperatorResultMetadata(1, "x")))
    val s2 = WorkflowResultStore(Map(opA -> OperatorResultMetadata(1, "x")))
    val s3 = WorkflowResultStore(Map(opA -> OperatorResultMetadata(2, "x")))
    val s4 = WorkflowResultStore(Map.empty)
    assert(s1 == s2)
    assert(s1.hashCode == s2.hashCode)
    assert(s1 != s3, "differing inner metadata must break equality")
    assert(s1 != s4, "differing map size must break equality")
  }

  // ---------------------------------------------------------------------------
  // copy semantics
  // ---------------------------------------------------------------------------

  "WorkflowResultStore.copy" should "replace the resultInfo map" in {
    val base = WorkflowResultStore(Map(opA -> OperatorResultMetadata(1, "x")))
    val updated = base.copy(resultInfo = Map(opB -> OperatorResultMetadata(2, "y")))
    assert(updated.resultInfo.keySet == Set(opB))
    assert(updated.resultInfo(opB) == OperatorResultMetadata(2, "y"))
    // Original is unchanged (immutable case-class semantics).
    assert(base.resultInfo.keySet == Set(opA))
  }

  // ---------------------------------------------------------------------------
  // Default-arg construction
  // ---------------------------------------------------------------------------

  "WorkflowResultStore (default-arg construction)" should
    "equal WorkflowResultStore(Map.empty) — the default arg is `Map.empty`" in {
    assert(WorkflowResultStore() == WorkflowResultStore(Map.empty))
  }
}
