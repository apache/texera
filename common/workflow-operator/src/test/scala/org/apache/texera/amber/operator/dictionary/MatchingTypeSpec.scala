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

package org.apache.texera.amber.operator.dictionary

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MatchingTypeSpec extends AnyFlatSpec with Matchers {

  "MatchingType" should "map each constant to its wire name via getName" in {
    MatchingType.SCANBASED.getName shouldBe "Scan"
    MatchingType.SUBSTRING.getName shouldBe "Substring"
    MatchingType.CONJUNCTION_INDEXBASED.getName shouldBe "Conjunction"
    MatchingType.values() should have length 3
  }

  "MatchingType" should "round-trip through Jackson using its wire name" in {
    objectMapper.writeValueAsString(MatchingType.SUBSTRING) shouldBe "\"Substring\""
    objectMapper.readValue("\"Conjunction\"", classOf[MatchingType]) shouldBe
      MatchingType.CONJUNCTION_INDEXBASED
  }
}
