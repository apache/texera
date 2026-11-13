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

package org.apache.texera.amber.operator.metadata

import org.scalatest.flatspec.AnyFlatSpec

class PropertyNameConstantsSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Logical-plan keys — each is a stable identifier used in every workflow
  // JSON we have ever shipped; a rename breaks every persisted plan.
  // ---------------------------------------------------------------------------

  "PropertyNameConstants logical-plan keys" should "have their canonical String values" in {
    assert(PropertyNameConstants.OPERATOR_ID == "operatorID")
    assert(PropertyNameConstants.OPERATOR_VERSION == "operatorVersion")
  }

  // ---------------------------------------------------------------------------
  // Distinctness — no two constants alias to the same string
  // ---------------------------------------------------------------------------

  "PropertyNameConstants" should "have all constants distinct (no accidental aliases)" in {
    val all = List(
      PropertyNameConstants.OPERATOR_ID,
      PropertyNameConstants.OPERATOR_VERSION
    )
    assert(all.distinct.size == all.size, s"duplicate constant value(s) in: $all")
  }

  it should "carry no leading/trailing whitespace on any constant" in {
    val all = List(
      PropertyNameConstants.OPERATOR_ID,
      PropertyNameConstants.OPERATOR_VERSION
    )
    all.foreach(c => assert(c == c.trim, s"constant has surrounding whitespace: '$c'"))
  }
}
