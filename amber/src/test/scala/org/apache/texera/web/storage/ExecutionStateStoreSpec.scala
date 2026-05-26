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

package org.apache.texera.web.storage

import org.apache.texera.amber.engine.common.executionruntimestate.{
  ExecutionBreakpointStore,
  ExecutionConsoleStore,
  ExecutionMetadataStore,
  ExecutionStatsStore
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExecutionStateStoreSpec extends AnyFlatSpec with Matchers {

  "ExecutionStateStore" should "initialise each child store with the empty proto default" in {
    val s = new ExecutionStateStore
    s.statsStore.getState shouldBe ExecutionStatsStore()
    s.metadataStore.getState shouldBe ExecutionMetadataStore()
    s.consoleStore.getState shouldBe ExecutionConsoleStore()
    s.breakpointStore.getState shouldBe ExecutionBreakpointStore()
    s.reconfigurationStore.getState shouldBe ExecutionReconfigurationStore()
  }

  // Order is part of the contract: WorkflowLifecycleManager subscribes
  // to metadataStore by position-independent name, but ExecutionResultService
  // iterates getAllStores when wiring diff handlers in bulk, so a re-order
  // would shuffle which handler runs against which state.
  "getAllStores" should "return all five stores in stats/console/breakpoint/metadata/reconfiguration order" in {
    val s = new ExecutionStateStore
    val stores = s.getAllStores.toList
    stores should have size 5
    stores(0) should be theSameInstanceAs s.statsStore
    stores(1) should be theSameInstanceAs s.consoleStore
    stores(2) should be theSameInstanceAs s.breakpointStore
    stores(3) should be theSameInstanceAs s.metadataStore
    stores(4) should be theSameInstanceAs s.reconfigurationStore
  }
}
