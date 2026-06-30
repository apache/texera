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

package org.apache.texera.amber.operator.aggregate

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AggregationFunctionSpec extends AnyFlatSpec with Matchers {

  "AggregationFunction" should "map each constant to its wire name via getName" in {
    AggregationFunction.SUM.getName shouldBe "sum"
    AggregationFunction.COUNT.getName shouldBe "count"
    AggregationFunction.AVERAGE.getName shouldBe "average"
    AggregationFunction.MIN.getName shouldBe "min"
    AggregationFunction.MAX.getName shouldBe "max"
    AggregationFunction.CONCAT.getName shouldBe "concat"
    AggregationFunction.values() should have length 6
  }

  "AggregationFunction" should "round-trip through Jackson using its wire name" in {
    objectMapper.writeValueAsString(AggregationFunction.AVERAGE) shouldBe "\"average\""
    objectMapper.readValue("\"concat\"", classOf[AggregationFunction]) shouldBe
      AggregationFunction.CONCAT
  }
}
