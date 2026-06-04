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

  // Output-port-level key. An operator with N output ports gets N entries
  // (e.g. Split has port 0 and port 1, each with its own assigned dfN var).
  private type PortKey = (String, Int) // (opId, portIdx)

  def translate(logicalPlan: LogicalPlan): String = {
    // Track downstream connections per (opId, fromPortIdx). A port is a leaf
    // if it has no outgoing edges — operator-level "no outgoing links" is too
    // coarse for multi-output ops (Split's port 0 may have downstream while
    // port 1 doesn't, or vice versa).
    val outgoingFromPort = mutable.Map[PortKey, Int]().withDefaultValue(0)
    logicalPlan.links.foreach { link =>
      outgoingFromPort((link.fromOpId.id, link.fromPortId.id)) += 1
    }

    val outputVar = mutable.Map[PortKey, String]()
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

      // Resolve upstream inputs in the consuming operator's input-port order
      // (link.toPortId), NOT the order links happen to appear in the plan's
      // link list. This makes in1df/in2df/... deterministic and correct for
      // multi-input operators (joins, set ops) where port 0 vs port 1 carries
      // semantics (e.g. build vs probe side). Ties on the same toPortId keep
      // link order — relevant for variadic single-port operators like Union.
      // Each upstream link is resolved via (fromOpId, fromPortId) so that a
      // multi-output upstream (Split) hands each downstream the correct DF.
      val inVars = logicalPlan
        .getUpstreamLinks(opIdentity)
        .sortBy(link => (link.toPortId.id, link.toPortId.internal))
        .map(link => outputVar((link.fromOpId.id, link.fromPortId.id)))

      // Allocate one dfN per declared output port. Existing single-output
      // operators have outputPorts.size == 1, so they get exactly one var and
      // their behavior is identical to the previous flat scheme.
      val outVars = op.operatorInfo.outputPorts.map { port =>
        val v = s"df$varCounter"
        varCounter += 1
        outputVar((opId, port.id.id)) = v
        v
      }

      script += s"# [$displayName]"

      // Jackson deserializes each operator into its concrete subclass via @JsonSubTypes on LogicalOp,
      // so the pattern match below will resolve to the correct descriptor (e.g. BarChartOpDesc).
      op match {
        case gen: StandaloneCodeGenerator =>
          // generateStandaloneCode() returns a code block using in{N}df / out{N}df
          // placeholders; substituteVars() replaces them with the assigned vars.
          script += substituteVars(gen.generateStandaloneCode(), inVars, outVars, displayName)

        case _ =>
          logger.warn(
            s"Operator '$displayName' does not implement StandaloneCodeGenerator. Skipping."
          )
          script += s"# TODO: '$displayName' is not yet supported by the translator."
          outVars.zipWithIndex.foreach {
            case (v, i) => script += s"# $v = <output port $i of $displayName>"
          }
      }

      script += ""
    }

    // Leaf detection runs at the port level: a (opId, port) pair is a leaf
    // if no link consumes it. For Split with one downstream port and one
    // dangling port, only the dangling port is treated as a leaf to print.
    val leafPorts = outputVar.keys.toList
      .sortBy { case (_, portIdx) => portIdx }
      .filter(key => outgoingFromPort(key) == 0)
    val dataFrameLeafPorts = leafPorts.filter {
      case (opId, _) =>
        logicalPlan.getOperator(opId) match {
          case gen: StandaloneCodeGenerator => gen.producesDataFrame()
          case _                            => false
        }
    }

    if (dataFrameLeafPorts.nonEmpty) {
      script += "# --- Output ---"
      // Print in topological order of the producing operator so multi-port
      // operators print contiguously and the order matches the script flow.
      val topoIndex = topoOrder.map(_.id).zipWithIndex.toMap
      dataFrameLeafPorts
        .sortBy { case (opId, portIdx) => (topoIndex.getOrElse(opId, Int.MaxValue), portIdx) }
        .foreach {
          case (opId, portIdx) =>
            val varName = outputVar((opId, portIdx))
            val displayName = logicalPlan.getOperator(opId).operatorInfo.userFriendlyName
            val portSuffix = if (outputVar.keys.count(_._1 == opId) > 1) s" port $portIdx" else ""
            script += s"""print("\\n[$displayName$portSuffix] $varName:")"""
            script += s"print($varName.head())"
            script += ""
        }
    }

    script.mkString("\n")
  }

  // Replaces in{N}df / out{N}df placeholders with concrete variable names.
  // Substitutes in reverse index order to prevent partial matches (e.g. in1df
  // inside in10df). After substitution, scans for any leftover placeholders
  // and logs a warning — that signals a mismatch between an operator's
  // declared port count and what its generateStandaloneCode actually emits.
  private def substituteVars(
                              code: String,
                              inVars: List[String],
                              outVars: List[String],
                              displayName: String
                            ): String = {
    var result = code

    inVars.zipWithIndex.reverse.foreach {
      case (v, idx) => result = result.replaceAll(s"\\bin${idx + 1}df\\b", v)
    }
    outVars.zipWithIndex.reverse.foreach {
      case (v, idx) => result = result.replaceAll(s"\\bout${idx + 1}df\\b", v)
    }

    val leftoverIn = """\bin\d+df\b""".r.findAllIn(result).toSet
    val leftoverOut = """\bout\d+df\b""".r.findAllIn(result).toSet
    if (leftoverIn.nonEmpty || leftoverOut.nonEmpty) {
      logger.warn(
        s"Operator '$displayName' emitted placeholders that don't match its port " +
          s"count: leftover inputs=$leftoverIn, leftover outputs=$leftoverOut. " +
          s"Generated script will reference unbound variables."
      )
    }

    result
  }
}
