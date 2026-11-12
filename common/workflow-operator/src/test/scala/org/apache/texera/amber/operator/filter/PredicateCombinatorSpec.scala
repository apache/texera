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

package org.apache.texera.amber.operator.filter

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PredicateCombinatorSpec extends AnyFlatSpec with Matchers {

  "PredicateCombinator" should "map each constant to its wire name via getName" in {
    PredicateCombinator.OR.getName shouldBe "any (OR)"
    PredicateCombinator.AND.getName shouldBe "all (AND)"
    PredicateCombinator.values() should have length 2
  }

  "PredicateCombinator.fromString" should "resolve names case-insensitively" in {
    PredicateCombinator.fromString("any (OR)") shouldBe PredicateCombinator.OR
    PredicateCombinator.fromString("ALL (AND)") shouldBe PredicateCombinator.AND
  }

  it should "reject an unknown name" in {
    intercept[IllegalArgumentException](PredicateCombinator.fromString("xor"))
  }

  "PredicateCombinator" should "round-trip through Jackson using its name" in {
    objectMapper.writeValueAsString(PredicateCombinator.AND) shouldBe "\"all (AND)\""
    objectMapper.readValue(
      "\"any (OR)\"",
      classOf[PredicateCombinator]
    ) shouldBe PredicateCombinator.OR
  }
}
