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

package org.apache.texera.amber.operator.hashJoin

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JoinTypeSpec extends AnyFlatSpec with Matchers {

  "JoinType" should "map each constant to its wire value via getJoinType" in {
    JoinType.INNER.getJoinType shouldBe "inner"
    JoinType.LEFT_OUTER.getJoinType shouldBe "left outer"
    JoinType.RIGHT_OUTER.getJoinType shouldBe "right outer"
    JoinType.FULL_OUTER.getJoinType shouldBe "full outer"
    JoinType.values() should have length 4
  }

  "JoinType" should "round-trip through Jackson using its wire value" in {
    objectMapper.writeValueAsString(JoinType.LEFT_OUTER) shouldBe "\"left outer\""
    objectMapper.readValue("\"full outer\"", classOf[JoinType]) shouldBe JoinType.FULL_OUTER
  }
}
