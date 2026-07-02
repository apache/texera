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

package org.apache.texera.amber.operator.machineLearning.Scorer

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RegressionMetricsFncSpec extends AnyFlatSpec with Matchers {

  "regressionMetricsFnc" should "map each constant to its wire value" in {
    regressionMetricsFnc.mse.getName shouldBe "MSE"
    regressionMetricsFnc.rmse.getName shouldBe "RMSE"
    regressionMetricsFnc.mae.getName shouldBe "MAE"
    regressionMetricsFnc.r2.getName shouldBe "R2"
    regressionMetricsFnc.values() should have length 4
  }

  "regressionMetricsFnc" should "round-trip through Jackson using its wire value" in {
    objectMapper.writeValueAsString(regressionMetricsFnc.rmse) shouldBe "\"RMSE\""
    objectMapper.readValue(
      "\"RMSE\"",
      classOf[regressionMetricsFnc]
    ) shouldBe regressionMetricsFnc.rmse
  }
}
