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

package org.apache.texera.amber.operator.visualization.rangeSlider

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RangeSliderHandleDuplicateFunctionSpec extends AnyFlatSpec with Matchers {

  "RangeSliderHandleDuplicateFunction" should "map each constant to its wire value" in {
    RangeSliderHandleDuplicateFunction.NOTHING.getFunctionType shouldBe "Nothing"
    RangeSliderHandleDuplicateFunction.MEAN.getFunctionType shouldBe "Mean"
    RangeSliderHandleDuplicateFunction.SUM.getFunctionType shouldBe "Sum"
    RangeSliderHandleDuplicateFunction.values() should have length 3
  }

  "RangeSliderHandleDuplicateFunction" should "round-trip through Jackson using its wire value" in {
    objectMapper.writeValueAsString(RangeSliderHandleDuplicateFunction.MEAN) shouldBe "\"Mean\""
    objectMapper.readValue(
      "\"Mean\"",
      classOf[RangeSliderHandleDuplicateFunction]
    ) shouldBe RangeSliderHandleDuplicateFunction.MEAN
  }
}
