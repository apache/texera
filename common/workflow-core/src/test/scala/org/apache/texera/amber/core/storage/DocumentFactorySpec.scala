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

package org.apache.texera.amber.core.storage

import org.apache.texera.amber.core.tuple.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import scala.collection.mutable

/**
  * Unit tests for `DocumentFactory.createOrReuseDocument`, the create-or-reuse
  * decision behind output-port storage provisioning.
  *
  * This is the branch that lets a re-executing region (a loop body) keep the
  * output an earlier run accumulated instead of clobbering it: a LoopEnd port's
  * region runs once per iteration, and `createDocument` overrides any existing
  * document, so on a re-run the existing document must be reused rather than
  * recreated.
  *
  * `exists` / `create` are injected so the four cases can be pinned directly
  * with a spy -- no iceberg backend, no live region.
  */
class DocumentFactorySpec extends AnyFlatSpec with Matchers {

  private val uri = new URI("vfs:///wf/result/loop-end")
  private val schema = Schema()

  /** Run createOrReuseDocument with a spy and return (created?, #create calls). */
  private def provision(reuseExisting: Boolean, exists: Boolean): (Boolean, Int) = {
    val createCalls = mutable.ArrayBuffer.empty[URI]
    val created = DocumentFactory.createOrReuseDocument(
      uri,
      schema,
      reuseExisting,
      _ => exists,
      (u, _) => { createCalls += u; () }
    )
    (created, createCalls.size)
  }

  "createOrReuseDocument" should
    "reuse (not recreate) an existing document when the port reuses storage" in {
    // The loop-iteration case: the document is already there from a prior
    // region run, so it must NOT be recreated -- otherwise the accumulated
    // output would be clobbered.
    val (created, createCalls) = provision(reuseExisting = true, exists = true)
    created shouldBe false
    createCalls shouldBe 0
  }

  it should "create the document when the port reuses storage but none exists yet" in {
    // First iteration: nothing to reuse, so it must be created.
    val (created, createCalls) = provision(reuseExisting = true, exists = false)
    created shouldBe true
    createCalls shouldBe 1
  }

  it should "always (re)create when the port does not reuse storage, even if a document exists" in {
    // Non-reuse ports get a fresh document every region execution; an existing
    // one is intentionally overwritten.
    val (created, createCalls) = provision(reuseExisting = false, exists = true)
    created shouldBe true
    createCalls shouldBe 1
  }

  it should "create when the port does not reuse storage and none exists" in {
    val (created, createCalls) = provision(reuseExisting = false, exists = false)
    created shouldBe true
    createCalls shouldBe 1
  }

  it should "not probe existence when the port does not reuse storage (create unconditionally)" in {
    // Short-circuit: a non-reuse port always recreates, so it must not even
    // probe for existence.
    var existsProbed = false
    DocumentFactory.createOrReuseDocument(
      uri,
      schema,
      reuseExisting = false,
      _ => { existsProbed = true; true },
      (_, _) => ()
    )
    existsProbed shouldBe false
  }
}
