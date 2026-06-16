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

import org.scalatest.flatspec.AnyFlatSpec

class OperatorResultMetadataSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Defaults
  // ---------------------------------------------------------------------------

  "OperatorResultMetadata()" should "default tupleCount to 0" in {
    assert(OperatorResultMetadata().tupleCount == 0)
  }

  it should "default changeDetector to the empty string" in {
    assert(OperatorResultMetadata().changeDetector == "")
  }

  // ---------------------------------------------------------------------------
  // Custom constructor values
  // ---------------------------------------------------------------------------

  "OperatorResultMetadata(...)" should "preserve a custom tupleCount" in {
    assert(OperatorResultMetadata(tupleCount = 42).tupleCount == 42)
  }

  it should "preserve a custom changeDetector" in {
    assert(OperatorResultMetadata(changeDetector = "abc").changeDetector == "abc")
  }

  it should "preserve both fields together" in {
    val m = OperatorResultMetadata(tupleCount = 7, changeDetector = "hash-x")
    assert(m.tupleCount == 7)
    assert(m.changeDetector == "hash-x")
  }

  // ---------------------------------------------------------------------------
  // Equality / hashCode (case class semantics)
  // ---------------------------------------------------------------------------

  "OperatorResultMetadata equality" should "compare both fields" in {
    val a = OperatorResultMetadata(1, "x")
    val b = OperatorResultMetadata(1, "x")
    val c = OperatorResultMetadata(1, "y")
    val d = OperatorResultMetadata(2, "x")
    assert(a == b)
    assert(a.hashCode == b.hashCode)
    assert(a != c, "differing changeDetector must break equality")
    assert(a != d, "differing tupleCount must break equality")
  }

  // ---------------------------------------------------------------------------
  // copy semantics
  // ---------------------------------------------------------------------------

  "OperatorResultMetadata.copy" should
    "replace only the field that was supplied, preserving the rest" in {
    val base = OperatorResultMetadata(5, "old-hash")
    assert(base.copy(tupleCount = 10) == OperatorResultMetadata(10, "old-hash"))
    assert(base.copy(changeDetector = "new-hash") == OperatorResultMetadata(5, "new-hash"))
  }
}
