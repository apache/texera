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
import org.apache.texera.amber.compiler.model.LogicalPlanPojo
import org.apache.texera.amber.operator.StandaloneCodeGenerator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class WorkflowToPythonTranslator extends LazyLogging {

  def translate(pojo: LogicalPlanPojo): String = {
    // TODO: filter disabled operators
    val activeOperators = pojo.operators
    val links = pojo.links

    val opIds = activeOperators.map(_.operatorIdentifier.id)

    val incoming = mutable.Map[String, ArrayBuffer[String]]()
    val outgoing = mutable.Map[String, ArrayBuffer[String]]()
    opIds.foreach { id =>
      incoming(id) = ArrayBuffer.empty
      outgoing(id) = ArrayBuffer.empty
    }
    links.foreach { link =>
      val src = link.fromOpId.id
      val tgt = link.toOpId.id
      // only wire links between active operators
      if (incoming.contains(src) && outgoing.contains(tgt)) {
        outgoing(src) += tgt
        incoming(tgt) += src
      }
    }

    val opById = activeOperators.map(op => op.operatorIdentifier.id -> op).toMap
    val order = topoSort(opIds, outgoing)

    val outputVar = mutable.Map[String, String]()
    var varCounter = 1
    val lines = ArrayBuffer[String]()

    lines += "import pandas as pd"
    lines += "import plotly.express as px"
    lines += "import plotly.graph_objects as go"
    lines += "import plotly.io"
    lines += ""

    for (opId <- order) {
      val op = opById(opId)
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

    val leafIds = order.filter(id => outgoing(id).isEmpty)
    val dataFrameLeaves = leafIds.filter { id =>
      opById(id) match {
        case gen: StandaloneCodeGenerator => gen.producesDataFrame()
        case _                            => false
      }
    }

    if (dataFrameLeaves.nonEmpty) {
      lines += "# --- Output ---"
      for (opId <- dataFrameLeaves) {
        val varName = outputVar(opId)
        val displayName = opById(opId).operatorInfo.userFriendlyName
        lines += s"""print("\\n[$displayName] $varName:")"""
        lines += s"print($varName.head())"
        lines += ""
      }
    }

    lines.mkString("\n")
  }

  private def topoSort(
      opIds: List[String],
      outgoing: mutable.Map[String, ArrayBuffer[String]]
  ): List[String] = {
    val inDegree = mutable.Map[String, Int]()
    opIds.foreach(id => inDegree(id) = 0)
    opIds.foreach(id => outgoing(id).foreach(tgt => inDegree(tgt) += 1))

    val queue = mutable.Queue[String]()
    opIds.filter(id => inDegree(id) == 0).foreach(queue.enqueue)

    val order = ArrayBuffer[String]()
    while (queue.nonEmpty) {
      val curr = queue.dequeue()
      order += curr
      outgoing(curr).foreach { next =>
        inDegree(next) -= 1
        if (inDegree(next) == 0) queue.enqueue(next)
      }
    }

    if (order.length != opIds.length)
      throw new IllegalArgumentException(
        "Workflow contains a cycle or disconnected operators."
      )

    order.toList
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
