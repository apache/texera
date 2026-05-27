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

import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.source.scan.json.JSONLScanSourceOpDesc
import org.apache.texera.amber.operator.visualization.dumbbellPlot.DumbbellPlotOpDesc
import org.apache.texera.amber.operator.visualization.filledAreaPlot.FilledAreaPlotOpDesc
import org.scalatest.flatspec.AnyFlatSpec

class OperatorBooleanDefaultSpec extends AnyFlatSpec {

  private def assertFalseDefault(
      opDescClass: Class[_ <: LogicalOp],
      propertyName: String
  ): Unit = {
    val propertySchema =
      OperatorMetadataGenerator
        .generateOperatorJsonSchema(opDescClass)
        .path("properties")
        .path(propertyName)

    assert(propertySchema.path("type").asText() == "boolean")
    assert(propertySchema.has("default"))
    assert(!propertySchema.path("default").asBoolean())
  }

  "Operator metadata generation" should "emit false defaults for visualization checkbox fields" in {
    assertFalseDefault(classOf[FilledAreaPlotOpDesc], "facetColumn")
    assertFalseDefault(classOf[DumbbellPlotOpDesc], "showLegends")
  }

  it should "emit a false default for JSONL flattening" in {
    assertFalseDefault(classOf[JSONLScanSourceOpDesc], "flatten")
  }
}
