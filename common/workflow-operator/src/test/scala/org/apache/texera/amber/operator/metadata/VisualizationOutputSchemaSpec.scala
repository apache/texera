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

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.visualization.PythonVisualizationOpDesc
import org.apache.texera.amber.operator.visualization.htmlviz.HtmlVizOpDesc
import org.apache.texera.amber.operator.visualization.urlviz.UrlVizOpDesc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Guard for the visualization output contract, over every registered
  * visualization descriptor.
  *
  * The frontend locates the chart HTML a visualization returns by reading a
  * single `html-content` STRING column from the operator's one output port, and
  * the exported script must not print a chart as a leaf DataFrame.
  * `PythonVisualizationOpDesc` declares both as final members, but a new chart
  * that does not extend it can still ship a schema the frontend cannot render,
  * and only that operator's own spec -- if it asserts the schema at all -- would
  * notice.
  *
  * Sweeping the registry rather than testing the descriptors one by one is
  * deliberate: a per-operator test would not exist for the next chart until
  * someone remembers to write it.
  */
class VisualizationOutputSchemaSpec extends AnyFlatSpec with Matchers {

  // HtmlViz and UrlViz are Scala map operators: they do not extend
  // PythonOperatorDescriptor and derive their output schema through
  // getPhysicalOp's SchemaPropagationFunc instead, covered by their own specs.
  // They are excluded by explicit class so that any NEW visualization
  // descriptor is swept by default and must be consciously added here to
  // escape the contract.
  private val nonPythonVisualizations: Set[Class[_]] =
    Set(classOf[HtmlVizOpDesc], classOf[UrlVizOpDesc])

  private val visualizationClasses =
    OperatorMetadataGenerator.operatorTypeMap.keys.toSeq
      .filter(_.getName.startsWith("org.apache.texera.amber.operator.visualization."))
      .sortBy(_.getSimpleName)

  private val sweptClasses =
    visualizationClasses.filterNot(nonPythonVisualizations.contains)

  private def instantiate(opClass: Class[_]): PythonVisualizationOpDesc =
    opClass.getConstructor().newInstance().asInstanceOf[PythonVisualizationOpDesc]

  "the registry" should "contain the visualization descriptors this sweep guards" in {
    // If the selection ever comes back empty (say, the package is renamed),
    // the per-operator assertions below would vacuously pass; pin a floor.
    sweptClasses.size should be >= 48
  }

  it should "register no visualization descriptor outside PythonVisualizationOpDesc except the known two" in {
    val outsideTheTrait =
      visualizationClasses.filterNot(classOf[PythonVisualizationOpDesc].isAssignableFrom)
    outsideTheTrait.toSet shouldBe nonPythonVisualizations
  }

  "every Python visualization descriptor" should "declare a single html-content STRING column on its one output port and not produce a DataFrame" in {
    sweptClasses.foreach { opClass =>
      withClue(s"${opClass.getSimpleName}: ") {
        val op = instantiate(opClass)
        val outputPortIds = op.operatorInfo.outputPorts.map(_.id)
        outputPortIds should have length 1

        val outputSchemas = op.getOutputSchemas(Map.empty[PortIdentity, Schema])
        outputSchemas.keySet shouldBe Set(outputPortIds.head)

        val schema = outputSchemas(outputPortIds.head)
        schema.getAttributeNames shouldBe List("html-content")
        schema.getAttribute("html-content").getType shouldBe AttributeType.STRING
        op.producesDataFrame() shouldBe false
      }
    }
  }

  it should "derive that schema independently of the input schemas" in {
    val populatedInput =
      Map(PortIdentity() -> Schema().add("x", AttributeType.INTEGER))
    sweptClasses.foreach { opClass =>
      withClue(s"${opClass.getSimpleName}: ") {
        val op = instantiate(opClass)
        op.getOutputSchemas(Map.empty[PortIdentity, Schema]) shouldBe
          op.getOutputSchemas(populatedInput)
      }
    }
  }
}
