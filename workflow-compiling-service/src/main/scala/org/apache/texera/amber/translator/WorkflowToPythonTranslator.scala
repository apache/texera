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
    val links = logicalPlan.links

    val incoming = mutable.Map[String, ArrayBuffer[String]]()
    val outgoing = mutable.Map[String, ArrayBuffer[String]]()
    logicalPlan.operators.foreach { op =>
      incoming(op.operatorIdentifier.id) = ArrayBuffer.empty
      outgoing(op.operatorIdentifier.id) = ArrayBuffer.empty
    }
    links.foreach { link =>
      val src = link.fromOpId.id
      val tgt = link.toOpId.id
      outgoing(src) += tgt
      incoming(tgt) += src
    }

    val outputVar = mutable.Map[String, String]()
    var varCounter = 1
    val lines = ArrayBuffer[String]()

    lines += "import pandas as pd"
    lines += "import plotly.express as px"
    lines += "import plotly.graph_objects as go"
    lines += "import plotly.io"
    lines += ""

    // getTopologicalOpIds() uses jgrapht internally — no need for a custom topo sort
    val topoOrder = logicalPlan.getTopologicalOpIds.asScala.toList

    for (opIdentity <- topoOrder) {
      val op = logicalPlan.getOperator(opIdentity)
      val opId = opIdentity.id
      val displayName = op.operatorInfo.userFriendlyName
      val inVars = incoming(opId).map(outputVar).toList
      val outVar = s"df$varCounter"
      varCounter += 1
      outputVar(opId) = outVar

      lines += s"# [$displayName]"

      // Each operator is already the correct concrete subclass (e.g. BarChartOpDesc)
      // because Jackson uses @JsonSubTypes on LogicalOp to deserialize the pojo.
      op match {
        case gen: StandaloneCodeGenerator =>
          lines += substituteVars(gen.generateStandaloneCode(), inVars, outVar)

        case _ =>
          logger.warn(
            s"Operator '$displayName' does not implement StandaloneCodeGenerator. Skipping."
          )
          lines += s"# TODO: '$displayName' is not yet supported by the translator."
          lines += s"# $outVar = <output of $displayName>"
      }

      lines += ""
    }

    val leafIds = topoOrder.map(_.id).filter(id => outgoing(id).isEmpty)
    val dataFrameLeaves = leafIds.filter { id =>
      logicalPlan.getOperator(id) match {
        case gen: StandaloneCodeGenerator => gen.producesDataFrame()
        case _                            => false
      }
    }

    if (dataFrameLeaves.nonEmpty) {
      lines += "# --- Output ---"
      for (opId <- dataFrameLeaves) {
        val varName = outputVar(opId)
        val displayName = logicalPlan.getOperator(opId).operatorInfo.userFriendlyName
        lines += s"""print("\\n[$displayName] $varName:")"""
        lines += s"print($varName.head())"
        lines += ""
      }
    }

    lines.mkString("\n")
  }

  // Replaces in1df/out1df placeholders with concrete variable names.
  // Reverse index order prevents partial matches (e.g. in1df inside in10df).
  private def substituteVars(code: String, inVars: List[String], outVar: String): String = {
    var result = code

    inVars.zipWithIndex.reverse.foreach {
      case (varName, idx) =>
        result = result.replaceAll(s"\\bin${idx + 1}df\\b", varName)
    }

    result.replaceAll("""\bout1df\b""", outVar)
  }
}
