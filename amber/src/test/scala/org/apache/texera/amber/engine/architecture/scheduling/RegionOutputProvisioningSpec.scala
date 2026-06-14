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

package org.apache.texera.amber.engine.architecture.scheduling

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import scala.collection.mutable

/**
  * Unit tests for `RegionExecutionCoordinator.provisionOutputDocument`, the
  * create-or-reuse decision behind output-port storage provisioning.
  *
  * This is the branch that lets a re-executing region (a loop body) keep the
  * output an earlier run accumulated instead of clobbering it: a LoopEnd's
  * region runs once per iteration, and `DocumentFactory.createDocument`
  * overrides any existing document, so on a re-run we must reuse the existing
  * document rather than recreate it.
  *
  * The decision was pulled out of the private `createOutputPortStorageObjects`
  * (which needs a live controller + iceberg backend) into a pure function with
  * injected `documentExists` / `createDocument`, so the four cases can be
  * pinned directly with a spy -- no iceberg, no actor system.
  */
class RegionOutputProvisioningSpec extends AnyFlatSpec with Matchers {

  private val uri = new URI("vfs:///wf/result/loop-end")

  /** Run provisionOutputDocument and return (created?, number of create calls). */
  private def provision(
      reuseExistingStorage: Boolean,
      exists: Boolean
  ): (Boolean, Int) = {
    val createCalls = mutable.ArrayBuffer.empty[URI]
    val created = RegionExecutionCoordinator.provisionOutputDocument(
      uri,
      reuseExistingStorage,
      _ => exists,
      u => { createCalls += u; () }
    )
    (created, createCalls.size)
  }

  "provisionOutputDocument" should
    "reuse (not recreate) an existing document when the operator reuses storage" in {
    // The loop-iteration case: the document is already there from a prior
    // region run, so createDocument must NOT be called -- otherwise the
    // accumulated output would be clobbered.
    val (created, createCalls) = provision(reuseExistingStorage = true, exists = true)
    created shouldBe false
    createCalls shouldBe 0
  }

  it should "create the document when the operator reuses storage but none exists yet" in {
    // First iteration: nothing to reuse, so it must be created.
    val (created, createCalls) = provision(reuseExistingStorage = true, exists = false)
    created shouldBe true
    createCalls shouldBe 1
  }

  it should "always (re)create when the operator does not reuse storage, even if a document exists" in {
    // Non-loop operators get a fresh document every region execution; an
    // existing one is intentionally overwritten.
    val (created, createCalls) = provision(reuseExistingStorage = false, exists = true)
    created shouldBe true
    createCalls shouldBe 1
  }

  it should "create when the operator does not reuse storage and none exists" in {
    val (created, createCalls) = provision(reuseExistingStorage = false, exists = false)
    created shouldBe true
    createCalls shouldBe 1
  }

  it should "not call documentExists when the operator does not reuse storage (create unconditionally)" in {
    // Short-circuit: a non-reuse operator always recreates, so it must not
    // even probe for existence.
    var existsProbed = false
    RegionExecutionCoordinator.provisionOutputDocument(
      uri,
      reuseExistingStorage = false,
      _ => { existsProbed = true; true },
      _ => ()
    )
    existsProbed shouldBe false
  }
}
