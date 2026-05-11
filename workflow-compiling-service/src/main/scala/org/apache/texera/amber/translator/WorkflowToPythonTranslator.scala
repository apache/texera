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

package org.apache.texera.amber.translator

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.compiler.model.LogicalPlan
import org.apache.texera.amber.operator.StandaloneCodeGenerator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class WorkflowToPythonTranslator extends LazyLogging {

  def translate(logicalPlan: LogicalPlan): String = {
    val incoming = mutable.Map[String, ArrayBuffer[String]]()
    val outgoing = mutable.Map[String, ArrayBuffer[String]]()

    logicalPlan.operators.foreach { op =>
      val id = op.operatorIdentifier.id
      incoming(id) = ArrayBuffer.empty
      outgoing(id) = ArrayBuffer.empty
    }

    logicalPlan.links.foreach { link =>
      outgoing(link.fromOpId.id) += link.toOpId.id
      incoming(link.toOpId.id) += link.fromOpId.id
    }

    val outputVar = mutable.Map[String, String]()
    var varCounter = 1
    val script = ArrayBuffer[String]()

    script += "import pandas as pd"
    script += "import plotly.express as px"
    script += "import plotly.graph_objects as go"
    script += "import plotly.io"
    script += ""

    // getTopologicalOpIds() uses jgrapht internally — no need for a custom topo sort
    val topoOrder = logicalPlan.getTopologicalOpIds.asScala.toList

    for (opIdentity <- topoOrder) {
      val opId = opIdentity.id
      val op = logicalPlan.getOperator(opIdentity)
      val displayName = op.operatorInfo.userFriendlyName
      val inVars = incoming(opId).map(outputVar).toList
      val outVar = s"df$varCounter"
      varCounter += 1
      outputVar(opId) = outVar

      script += s"# [$displayName]"

      // Jackson deserializes each operator into its concrete subclass via @JsonSubTypes on LogicalOp,
      // so the pattern match below will resolve to the correct descriptor (e.g. BarChartOpDesc).
      op match {
        case gen: StandaloneCodeGenerator =>
          // generateStandaloneCode() returns a code block using in1df/out1df as placeholder
          // variable names, which substituteVars() replaces with the actual assigned variable names.
          script += substituteVars(gen.generateStandaloneCode(), inVars, outVar)

        case _ =>
          logger.warn(
            s"Operator '$displayName' does not implement StandaloneCodeGenerator. Skipping."
          )
          script += s"# TODO: '$displayName' is not yet supported by the translator."
          script += s"# $outVar = <output of $displayName>"
      }

      script += ""
    }

    // Print leaf operator outputs that produce DataFrames so the script
    // gives visible output when run locally.
    val leafIds = topoOrder.map(_.id).filter(id => outgoing(id).isEmpty)
    val dataFrameLeaves = leafIds.filter { id =>
      logicalPlan.getOperator(id) match {
        case gen: StandaloneCodeGenerator => gen.producesDataFrame()
        case _                            => false
      }
    }

    if (dataFrameLeaves.nonEmpty) {
      script += "# --- Output ---"
      for (opId <- dataFrameLeaves) {
        val varName = outputVar(opId)
        val displayName = logicalPlan.getOperator(opId).operatorInfo.userFriendlyName
        script += s"""print("\\n[$displayName] $varName:")"""
        script += s"print($varName.head())"
        script += ""
      }
    }

    script.mkString("\n")
  }

  // Replaces in1df/out1df placeholders with concrete variable names.
  // Substitutes in reverse index order to prevent partial matches (e.g. in1df inside in10df).
  private def substituteVars(code: String, inVars: List[String], outVar: String): String = {
    var result = code

    inVars.zipWithIndex.reverse.foreach {
      case (varName, idx) =>
        result = result.replaceAll(s"\\bin${idx + 1}df\\b", varName)
    }

    result.replaceAll("""\bout1df\b""", outVar)
  }
}
