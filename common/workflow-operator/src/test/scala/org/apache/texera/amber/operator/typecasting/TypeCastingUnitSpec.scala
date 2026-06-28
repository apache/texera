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

package org.apache.texera.amber.operator.typecasting

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TypeCastingUnitSpec extends AnyFlatSpec with Matchers {

  "TypeCastingUnit" should "expose its attribute and result-type fields" in {
    val u = new TypeCastingUnit
    u.attribute = "amount"
    u.resultType = AttributeType.DOUBLE
    u.attribute shouldBe "amount"
    u.resultType shouldBe AttributeType.DOUBLE
  }

  "TypeCastingUnit" should "round-trip its fields through Jackson" in {
    val u = new TypeCastingUnit
    u.attribute = "amount"
    u.resultType = AttributeType.INTEGER
    val json = objectMapper.writeValueAsString(u)
    val node = objectMapper.readTree(json)
    node.get("attribute").asText shouldBe "amount"
    node.get("resultType").asText shouldBe "integer"
    val restored = objectMapper.readValue(json, classOf[TypeCastingUnit])
    restored.attribute shouldBe "amount"
    restored.resultType shouldBe AttributeType.INTEGER
  }
}
