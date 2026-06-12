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

package org.apache.texera.amber.translator.verify

import org.apache.texera.amber.core.workflow.PortIdentity
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class CanonicalFixtureSpec extends AnyFlatSpec with Matchers {

  "CanonicalFixture" should "have at least 25 rows per port with partial id overlap" in {
    CanonicalFixture.port0Rows.size should be >= 25
    CanonicalFixture.port1Rows.size should be >= 25
    val ids0 = CanonicalFixture.port0Rows.map(_.getField[Integer]("id")).toSet
    val ids1 = CanonicalFixture.port1Rows.map(_.getField[Integer]("id")).toSet
    (ids0 intersect ids1) should not be empty
    (ids0 diff ids1) should not be empty
    (ids1 diff ids0) should not be empty
  }

  it should "contain the canonical value \"1\" in some but not all name cells" in {
    val names = CanonicalFixture.port0Rows.map(_.getField[String]("name"))
    names.count(_ == "1") should be > 0
    names.count(_ == "1") should be < names.size
  }

  it should "write one JSONL fixture per requested input port" in {
    val root = Files.createTempDirectory("canonical-fixture-")
    val inputs = CanonicalFixture.writeInputs(root, inputPortCount = 2)
    inputs.keySet shouldBe Set(PortIdentity(0), PortIdentity(1))
    inputs.values.foreach(p => Files.size(p) should be > 0L)
  }

  it should "reject unsupported port counts" in {
    val root = Files.createTempDirectory("canonical-fixture-")
    an[IllegalArgumentException] should be thrownBy
      CanonicalFixture.writeInputs(root, inputPortCount = 3)
  }
}
