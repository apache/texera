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

package org.apache.texera.amber.operator.visualization.candlestickChart

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CandlestickChartOpDescSpec extends AnyFlatSpec with Matchers {

  private def configured(): CandlestickChartOpDesc = {
    val d = new CandlestickChartOpDesc
    d.date = "day"
    d.open = "o"
    d.high = "h"
    d.low = "l"
    d.close = "c"
    d
  }

  "CandlestickChartOpDesc.operatorInfo" should
    "advertise the name and Financial visualization group" in {
    val info = (new CandlestickChartOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Candlestick Chart"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_FINANCIAL_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "CandlestickChartOpDesc" should "default all OHLC column fields to the empty string" in {
    val d = new CandlestickChartOpDesc
    d.date shouldBe ""
    d.open shouldBe ""
    d.high shouldBe ""
    d.low shouldBe ""
    d.close shouldBe ""
  }

  "CandlestickChartOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new CandlestickChartOpDesc
    val out = op.getOutputSchemas(Map(op.operatorInfo.inputPorts.head.id -> Schema()))
    out shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "CandlestickChartOpDesc.generatePythonCode" should "emit a Plotly Candlestick figure" in {
    val code = configured().generatePythonCode()
    code should include("go.Candlestick(")
    code should include("html-content")
  }

  "CandlestickChartOpDesc" should "round-trip its OHLC fields through the polymorphic base" in {
    val restored =
      objectMapper.readValue(objectMapper.writeValueAsString(configured()), classOf[LogicalOp])
    restored shouldBe a[CandlestickChartOpDesc]
    val c = restored.asInstanceOf[CandlestickChartOpDesc]
    c.date shouldBe "day"
    c.open shouldBe "o"
    c.high shouldBe "h"
    c.low shouldBe "l"
    c.close shouldBe "c"
  }
}
