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

package org.apache.texera.amber.core

import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WorkflowRuntimeExceptionSpec extends AnyFlatSpec with Matchers {

  private val worker = ActorVirtualIdentity("Worker:WF1-myOp-main-0")

  "WorkflowRuntimeException(message)" should "carry the message and default to no related worker" in {
    val ex = new WorkflowRuntimeException("boom")
    ex.message shouldBe "boom"
    ex.relatedWorkerId shouldBe None
    ex.getCause shouldBe null
  }

  "WorkflowRuntimeException(message, cause, workerId)" should "preserve message, attach cause, and record the worker" in {
    val cause = new IllegalStateException("inner")
    val ex = new WorkflowRuntimeException("outer", cause, Some(worker))
    ex.message shouldBe "outer"
    ex.getCause should be theSameInstanceAs cause
    ex.relatedWorkerId shouldBe Some(worker)
  }

  "WorkflowRuntimeException(cause, workerId)" should "derive the message from cause.toString" in {
    val cause = new IllegalArgumentException("bad arg")
    val ex = new WorkflowRuntimeException(cause, Some(worker))
    ex.message shouldBe cause.toString
    ex.getCause should be theSameInstanceAs cause
    ex.relatedWorkerId shouldBe Some(worker)
  }

  "WorkflowRuntimeException(cause)" should "derive the message and leave the worker unset" in {
    val cause = new RuntimeException("inner")
    val ex = new WorkflowRuntimeException(cause)
    ex.message shouldBe cause.toString
    ex.getCause should be theSameInstanceAs cause
    ex.relatedWorkerId shouldBe None
  }

  it should "fall back to a null message when the cause is null" in {
    // Pin: `Option(cause).map(_.toString).orNull` returns null for a null
    // cause, which then propagates into RuntimeException(null) — the parent
    // exception accepts that and reports getMessage as null.
    val ex = new WorkflowRuntimeException(null: Throwable)
    ex.message shouldBe null
    ex.getCause shouldBe null
  }

  "WorkflowRuntimeException()" should "produce a message-less exception with no cause and no worker" in {
    val ex = new WorkflowRuntimeException()
    ex.message shouldBe null
    ex.relatedWorkerId shouldBe None
    ex.getCause shouldBe null
  }

  "toString" should "return the raw message field rather than the JVM default" in {
    // The override returns `message` (or null) — not RuntimeException's
    // default `<class>: <message>` format.
    val ex = new WorkflowRuntimeException("oops")
    ex.toString shouldBe "oops"
  }
}
