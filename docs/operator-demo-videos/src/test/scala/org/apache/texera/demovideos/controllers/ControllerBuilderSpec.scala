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

package org.apache.texera.demovideos.controllers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

class ControllerBuilderSpec extends AnyFlatSpec with Matchers {

  // The steps under test never touch the page, so no Playwright instance is needed.
  private def newContext: ControllerContext = new ControllerContext(null)

  private class TestBuilder(ctx: ControllerContext) extends ControllerBuilder(ctx) {
    def add(name: String)(action: ControllerContext => Unit): this.type =
      addStep(ControllerStep(name)(action))
  }

  "execute" should "run each accumulated step exactly once, in insertion order" in {
    val ran = ArrayBuffer.empty[String]
    val builder = new TestBuilder(newContext)
    builder
      .add("first")(_ => ran += "first")
      .add("second")(_ => ran += "second")
      .add("third")(_ => ran += "third")
    builder.execute()
    ran.toList shouldBe List("first", "second", "third")
  }

  it should "stop at a failing step and not run the steps after it" in {
    val ran = ArrayBuffer.empty[String]
    val builder = new TestBuilder(newContext)
    builder
      .add("before")(_ => ran += "before")
      .add("boom")(_ => throw new IllegalStateException("boom"))
      .add("after")(_ => ran += "after")
    val e = intercept[IllegalStateException] {
      builder.execute()
    }
    e.getMessage shouldBe "boom"
    ran.toList shouldBe List("before")
  }

  it should "pass the builder's own context to every step" in {
    val ctx = newContext
    val seen = ArrayBuffer.empty[ControllerContext]
    val builder = new TestBuilder(ctx)
    builder.add("capture")(seen += _).add("capture again")(seen += _)
    builder.execute()
    seen.forall(_ eq ctx) shouldBe true
    seen should have size 2
  }
}
