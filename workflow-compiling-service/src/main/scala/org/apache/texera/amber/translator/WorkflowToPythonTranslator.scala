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
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.common.compiler.model.LogicalPlan
import org.apache.texera.amber.operator.StandaloneCodeGenerator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class WorkflowToPythonTranslator extends LazyLogging {

  // Output-port-level key. An operator with N output ports gets N entries
  // (e.g. Split has port 0 and port 1, each with its own assigned dfN var).
  private type PortKey = (String, Int) // (opId, portIdx)

  /**
    * @param outputSchemas what each operator's output ports carry, as
    *                      [[org.apache.texera.common.compiler.WorkflowCompiler]]
    *                      reports it. Folded into input-port schemas for the
    *                      generators that need a column's declared type, which the
    *                      file the script reads cannot carry. Empty is allowed.
    */
  def translate(
      logicalPlan: LogicalPlan,
      outputSchemas: Map[OperatorIdentity, Map[PortIdentity, Option[Schema]]] = Map.empty
  ): String = {
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
    // How many operators of each name have already been given output files. The
    // whole plan runs as one program in one directory, so a chart naming its own
    // file would be overwritten by the next chart naming the same one.
    val fileBaseCounts = mutable.Map[String, Int]()
    val script = ArrayBuffer[String]()

    // getTopologicalOpIds() uses jgrapht internally — no need for a custom topo sort
    val topoOrder = logicalPlan.getTopologicalOpIds.asScala.toList

    // pandas is the one module every generator uses: an operator body reads and
    // writes frames whatever else it does. Everything beyond that is asked of the
    // operators in the plan, so a script that draws nothing does not require a
    // plotting library to start.
    script += "import pandas as pd"
    topoOrder
      .map(logicalPlan.getOperator)
      .collect { case gen: StandaloneCodeGenerator => gen.standaloneImports() }
      .flatten
      .distinct
      .foreach(script += _)
    script += ""

    // Helper definitions the operator bodies below refer to. Collected across the
    // whole plan and deduplicated by text, so a workflow holding two operators
    // that share one helper still emits it once. Order follows the topological
    // order, which keeps the script stable for a given plan.
    val helpers = topoOrder
      .map(logicalPlan.getOperator)
      .collect { case gen: StandaloneCodeGenerator => gen.standaloneHelpers() }
      .flatten
      .distinct
    if (helpers.nonEmpty) {
      helpers.foreach { helper => script += helper; script += "" }
    }

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
          // Each upstream link carries its source port's schema to the port it
          // arrives at. An unresolved source is left out rather than guessed at.
          val inputSchemas = logicalPlan
            .getUpstreamLinks(opIdentity)
            .flatMap { link =>
              outputSchemas
                .get(link.fromOpId)
                .flatMap(_.get(link.fromPortId))
                .flatten
                .map(link.toPortId -> _)
            }
            .toMap

          // generateStandaloneCode() returns a code block using in{N}df / out{N}df
          // placeholders; substituteVars() replaces them with the assigned vars.
          script += substituteVars(
            gen.generateStandaloneCode(inputSchemas),
            inVars,
            outVars,
            fileBase(displayName, fileBaseCounts),
            displayName
          )

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
        logicalPlan.getOperator(OperatorIdentity(opId)) match {
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
            val displayName =
              logicalPlan.getOperator(OperatorIdentity(opId)).operatorInfo.userFriendlyName
            val portSuffix = if (outputVar.keys.count(_._1 == opId) > 1) s" port $portIdx" else ""
            script += s"""print("\\n[$displayName$portSuffix] $varName:")"""
            // The frame itself rather than head(): pandas already elides the
            // middle of a long one, and it states the row and column count,
            // which head() hides.
            script += s"print($varName)"
            script += ""
        }
    }

    script.mkString("\n")
  }

  // The stem of the files one operator writes, taken from its name so a reader
  // can tell whose picture is whose. Numbered from one even when the plan holds
  // a single chart, so the name a script writes does not depend on what else
  // the plan happens to contain.
  private def fileBase(displayName: String, counts: mutable.Map[String, Int]): String = {
    val slug = displayName.toLowerCase.replaceAll("[^a-z0-9]+", "_").replaceAll("^_|_$", "")
    val stem = if (slug.isEmpty) "output" else slug
    val n = counts.getOrElse(stem, 0) + 1
    counts(stem) = n
    s"${stem}_$n"
  }

  // Replaces in{N}df / out{N}df placeholders with concrete variable names.
  // Substitutes in reverse index order to prevent partial matches (e.g. in1df
  // inside in10df). Only the code parts are rewritten: a generator writes a
  // column name as a string literal, and a column may be named `in1df`.
  // After substitution, scans the code for any leftover placeholders and logs
  // a warning — that signals a mismatch between an operator's declared port
  // count and what its generateStandaloneCode actually emits.
  private def substituteVars(
      code: String,
      inVars: List[String],
      outVars: List[String],
      fileBase: String,
      displayName: String
  ): String = {
    def substitute(fragment: String): String = {
      var result = fragment

      // An operator that writes a file names it outputHtml or outputJson and
      // gets back a name of its own, for the reason given where fileBaseCounts
      // is declared. The stem holds letters, digits and underscores only, so it
      // carries nothing replaceAll would read as a group reference.
      result = result.replaceAll("""\boutputHtml\b""", "\"" + fileBase + ".html\"")
      result = result.replaceAll("""\boutputJson\b""", "\"" + fileBase + ".json\"")

      // A variadic port takes as many upstream links as the user draws, and an
      // operator reading one cannot name them: `in1df`/`in2df` state a count, and
      // whichever count it states is wrong for every other workflow. This one
      // placeholder becomes the whole list, so the operator writes the same line
      // whether it is fed one table or five.
      result = result.replaceAll("""\binAlldf\b""", inVars.mkString("[", ", ", "]"))
      inVars.zipWithIndex.reverse.foreach {
        case (v, idx) => result = result.replaceAll(s"\\bin${idx + 1}df\\b", v)
      }
      outVars.zipWithIndex.reverse.foreach {
        case (v, idx) => result = result.replaceAll(s"\\bout${idx + 1}df\\b", v)
      }
      result
    }

    val substituted = splitOffLiterals(code).map {
      case (isCode, text) => (isCode, if (isCode) substitute(text) else text)
    }

    // The code segments are joined by a newline for the scan, so the text
    // either side of a skipped literal cannot spell a placeholder nobody wrote.
    val scanned = substituted.collect { case (true, text) => text }.mkString("\n")
    val leftoverIn = """\bin\d+df\b""".r.findAllIn(scanned).toSet
    val leftoverOut = """\bout\d+df\b""".r.findAllIn(scanned).toSet
    if (leftoverIn.nonEmpty || leftoverOut.nonEmpty) {
      logger.warn(
        s"Operator '$displayName' emitted placeholders that don't match its port " +
          s"count: leftover inputs=$leftoverIn, leftover outputs=$leftoverOut. " +
          s"Generated script will reference unbound variables."
      )
    }

    substituted.map(_._2).mkString
  }

  // Splits a block into (isCode, text) segments, where a plain string literal
  // and a comment are not code and everything else is. An f-string counts as
  // code: its braces hold expressions, and a generator that renders a column
  // name renders it as a plain literal.
  private def splitOffLiterals(code: String): List[(Boolean, String)] = {
    val segments = ArrayBuffer[(Boolean, String)]()
    val pending = new StringBuilder
    var i = 0

    def flushCode(): Unit = {
      if (pending.nonEmpty) {
        segments += ((true, pending.toString))
        pending.setLength(0)
      }
    }

    while (i < code.length) {
      val c = code.charAt(i)
      if (c == '#') {
        flushCode()
        val newline = code.indexOf('\n', i)
        val end = if (newline < 0) code.length else newline
        segments += ((false, code.substring(i, end)))
        i = end
      } else if (c == '\'' || c == '"') {
        val triple = c.toString * 3
        val delim = if (code.startsWith(triple, i)) triple else c.toString
        val end = endOfLiteral(code, i + delim.length, delim)
        if (isFormatted(code, i)) pending ++= code.substring(i, end)
        else {
          flushCode()
          segments += ((false, code.substring(i, end)))
        }
        i = end
      } else {
        pending += c
        i += 1
      }
    }
    flushCode()
    segments.toList
  }

  // Whether the quote at `quoteIdx` opens an f-string. Its prefix is whatever
  // letters run up to it; nothing else may sit against a quote in Python.
  private def isFormatted(code: String, quoteIdx: Int): Boolean = {
    var j = quoteIdx
    var formatted = false
    while (j > 0 && code.charAt(j - 1).isLetter) {
      j -= 1
      if (code.charAt(j) == 'f' || code.charAt(j) == 'F') formatted = true
    }
    formatted
  }

  // The index just past the closing delimiter, or the end of the block if the
  // literal is never closed. A backslash escapes the next character even in a
  // raw string, where it still keeps the quote from closing the literal.
  private def endOfLiteral(code: String, from: Int, delim: String): Int = {
    var j = from
    var end = -1
    while (end < 0 && j < code.length) {
      if (code.charAt(j) == '\\') j += 2
      else if (code.startsWith(delim, j)) end = j + delim.length
      else j += 1
    }
    if (end < 0) code.length else end
  }
}
