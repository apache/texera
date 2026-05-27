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

import org.apache.texera.amber.core.storage.result.WorkflowResultStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WorkflowStateStoreSpec extends AnyFlatSpec with Matchers {

  "WorkflowStateStore" should "initialise resultStore with an empty WorkflowResultStore" in {
    val s = new WorkflowStateStore
    s.resultStore.getState shouldBe WorkflowResultStore()
    s.resultStore.getState.resultInfo shouldBe empty
  }

  "getAllStores" should "expose resultStore and only resultStore" in {
    val s = new WorkflowStateStore
    val stores = s.getAllStores.toList
    stores should have size 1
    stores.head should be theSameInstanceAs s.resultStore
  }
}
