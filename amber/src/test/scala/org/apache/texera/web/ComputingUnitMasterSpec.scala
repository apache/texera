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

package org.apache.texera.web

import org.apache.commons.jcs3.access.exception.InvalidArgumentException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ComputingUnitMasterSpec extends AnyFlatSpec with Matchers {

  "parseArgs" should "return no options when the master receives no arguments" in {
    ComputingUnitMaster.parseArgs(Array.empty[String]) shouldBe Map.empty
  }

  it should "parse a true cluster flag into a boolean" in {
    ComputingUnitMaster.parseArgs(Array("--cluster", "true")) shouldBe
      Map(Symbol("cluster") -> true)
  }

  it should "parse a false cluster flag into a boolean" in {
    ComputingUnitMaster.parseArgs(Array("--cluster", "false")) shouldBe
      Map(Symbol("cluster") -> false)
  }

  it should "use the last cluster value when the option is repeated" in {
    ComputingUnitMaster.parseArgs(
      Array("--cluster", "true", "--cluster", "false")
    ) shouldBe
      Map(Symbol("cluster") -> false)
  }

  it should "reject an unknown command-line option" in {
    val exception = intercept[InvalidArgumentException] {
      ComputingUnitMaster.parseArgs(Array("--serverAddr", "master.internal:8080"))
    }

    exception.getMessage shouldBe "unknown command-line arg"
  }

  it should "reject a cluster option with no value" in {
    val exception = intercept[InvalidArgumentException] {
      ComputingUnitMaster.parseArgs(Array("--cluster"))
    }

    exception.getMessage shouldBe "unknown command-line arg"
  }

  it should "parse a mixed-case cluster value case-insensitively" in {
    ComputingUnitMaster.parseArgs(Array("--cluster", "TRUE")) shouldBe
      Map(Symbol("cluster") -> true)
  }

  it should "reject an unknown option that follows a valid cluster pair" in {
    val exception = intercept[InvalidArgumentException] {
      ComputingUnitMaster.parseArgs(Array("--cluster", "true", "--bogus"))
    }

    exception.getMessage shouldBe "unknown command-line arg"
  }

  it should "consume a repeated flag as the cluster value" in {
    // The two-element pattern greedily takes the next token as the value, so a second
    // "--cluster" is fed to String.toBoolean and fails with IllegalArgumentException
    // rather than InvalidArgumentException (current behavior). This would flip to
    // InvalidArgumentException if a value-looks-like-a-flag guard were ever added.
    an[IllegalArgumentException] should be thrownBy {
      ComputingUnitMaster.parseArgs(Array("--cluster", "--cluster"))
    }
  }

  it should "fail with an IllegalArgumentException on a non-boolean cluster value" in {
    // The value is parsed with String.toBoolean, so a malformed boolean surfaces as an
    // IllegalArgumentException instead of the InvalidArgumentException used for unknown
    // options (current behavior).
    an[IllegalArgumentException] should be thrownBy {
      ComputingUnitMaster.parseArgs(Array("--cluster", "notabool"))
    }
  }
}
