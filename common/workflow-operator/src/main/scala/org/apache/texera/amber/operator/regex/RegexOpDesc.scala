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

package org.apache.texera.amber.operator.regex

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp, PortIdentity}
import org.apache.texera.amber.operator.{StandaloneCodeGenerator, StandaloneHelpers}
import org.apache.texera.amber.operator.filter.FilterOpDesc
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

class RegexOpDesc extends FilterOpDesc with StandaloneCodeGenerator {

  @JsonProperty(value = "attribute", required = true)
  @JsonPropertyDescription("column to search regex on")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(value = "regex", required = true)
  @JsonPropertyDescription("regular expression")
  var regex: String = _

  @JsonProperty(required = false, defaultValue = "false")
  @JsonSchemaTitle("Case Insensitive")
  @JsonPropertyDescription("regex match is case sensitive")
  var caseInsensitive: Boolean = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.regex.RegexOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Regular Expression",
      operatorDescription = "Search a regular expression in a string column",
      operatorGroupName = OperatorGroupConstants.SEARCH_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )

  override def generateStandaloneCode(): String = generateStandaloneCode(Map.empty)

  override def generateStandaloneCode(inputSchemas: Map[PortIdentity, Schema]): String = {
    // JVM uses Java Pattern.matcher(v).find — partial match. pandas str.contains
    // is also partial by default. Java-only regex syntax (\Q\E, possessive
    // quantifiers, etc.) may behave differently in Python's re engine.
    //
    // Pattern without UNICODE_CASE ignores case for ASCII letters only, and its
    // \w, \d and \s are ASCII, where Python's re is Unicode for all four, so the
    // pattern runs under re.ASCII. Java 17's \b still takes any letter or digit
    // as part of a word, so \b and \B keep Python's Unicode meaning.
    val pyLiteral = pyStringLiteral(
      RegexOpDesc.unicodeBoundaries(RegexOpDesc.unicodeCase(Option(regex).getOrElse("")))
    )
    val caseArg = if (caseInsensitive) "False" else "True"
    val attrLit = pyStringLiteral(attribute)
    // The rows with nothing in the column are dropped before the match rather than
    // left to `na=False`, which by then has no null to see.
    //
    // The pattern is matched against the text the engine would have matched, not
    // against whatever pandas made of the column. See [[renderedAsText]].
    val declared = inputSchemas.values.headOption
      .flatMap(schema => scala.util.Try(schema.getAttribute(attribute)).toOption)
      .map(_.getType)
    val column = renderedAsText(s"in1df[$attrLit]", declared)
    s"""import re
       |out1df = in1df[in1df[$attrLit].notna() & $column.str.contains($pyLiteral, regex=True, case=$caseArg, flags=re.ASCII, na=False)].reset_index(drop=True)""".stripMargin
  }

  override def standaloneHelpers(): Seq[String] = Seq(StandaloneHelpers.AttributeCasts)
}

object RegexOpDesc {

  private val LeadingFlags = """^(?:\(\?[a-zA-Z]*(?:-[a-zA-Z]*)?\))+""".r
  private val FlagGroup = """\(\?([a-zA-Z]*)(?:-([a-zA-Z]*))?\)""".r
  private val TurnsUnicodeOn = """\(\?[a-zA-Z]*u[a-zA-Z]*(?:-[a-zA-Z]*)?[:)]""".r
  private val Shorthands = "wWdDsS"

  /** Java's inline u flag folds case by Unicode and changes nothing else, while
    * Python refuses a global u beside re.ASCII and its u also widens \w, \d and
    * \s. So a leading u becomes a (?u:...) scope around the rest, and once any u
    * is on, each \w, \d and \s is scoped back to ASCII. A class holding one is
    * split, so its other letters still fold.
    */
  def unicodeCase(pattern: String): String = {
    if (TurnsUnicodeOn.findFirstIn(pattern).isEmpty) return pattern
    val lead = LeadingFlags.findPrefixOf(pattern).getOrElse("")
    val leadGroups = FlagGroup.findAllMatchIn(lead).toList
    val leadOn = leadGroups.exists(_.group(1).contains('u'))
    val out = new StringBuilder
    leadGroups.foreach { g =>
      val on = g.group(1).filterNot(_ == 'u')
      val off = Option(g.group(2)).getOrElse("").filterNot(_ == 'u')
      if (on.nonEmpty || off.nonEmpty) out.append(s"(?$on${if (off.isEmpty) "" else s"-$off"})")
    }
    if (leadOn) out.append("(?u:")

    var i = lead.length
    while (i < pattern.length) {
      val c = pattern.charAt(i)
      if (c == '\\' && i + 1 < pattern.length) {
        val next = pattern.charAt(i + 1)
        if (Shorthands.contains(next)) out.append(s"(?a:\\$next)")
        else out.append(c).append(next)
        i += 2
      } else if (c == '[') {
        var j = i + 1
        val negated = j < pattern.length && pattern.charAt(j) == '^'
        if (negated) j += 1
        val others = new StringBuilder
        val ascii = new StringBuilder
        // A ']' right after '[' or '[^' is a literal, not the end.
        if (j < pattern.length && pattern.charAt(j) == ']') { others.append(']'); j += 1 }
        while (j < pattern.length && pattern.charAt(j) != ']') {
          if (pattern.charAt(j) == '\\' && j + 1 < pattern.length) {
            val next = pattern.charAt(j + 1)
            (if (Shorthands.contains(next)) ascii else others).append('\\').append(next)
            j += 2
          } else { others.append(pattern.charAt(j)); j += 1 }
        }
        if (ascii.isEmpty || j >= pattern.length)
          out.append(pattern.substring(i, math.min(j + 1, pattern.length)))
        else {
          val rest = if (others.toString.startsWith("^")) "\\" + others else others.toString
          out.append(
            (negated, rest.isEmpty) match {
              case (false, true)  => s"(?a:[$ascii])"
              case (false, false) => s"(?:[$rest]|(?a:[$ascii]))"
              case (true, true)   => s"(?a:[^$ascii])"
              case (true, false)  => s"(?:(?![$rest])(?a:[^$ascii]))"
            }
          )
        }
        i = j + 1
      } else {
        out.append(c)
        i += 1
      }
    }
    // Under the x flag a trailing comment would swallow the closing parenthesis.
    if (leadOn) out.append(if (leadGroups.exists(_.group(1).contains('x'))) "\n)" else ")")
    out.toString
  }

  /** Rewrites each \b and \B outside a character class as a group scoped to
    * Unicode, so it keeps that meaning under re.ASCII. An escaped character is
    * copied whole, so "\\b" stays a backslash followed by b.
    */
  def unicodeBoundaries(pattern: String): String = {
    val out = new StringBuilder
    var inClass = false
    var i = 0
    while (i < pattern.length) {
      val c = pattern.charAt(i)
      if (c == '\\' && i + 1 < pattern.length) {
        val next = pattern.charAt(i + 1)
        if (!inClass && (next == 'b' || next == 'B')) out.append(s"(?u:\\$next)")
        else out.append(c).append(next)
        i += 2
      } else {
        if (c == '[' && !inClass) {
          inClass = true
          out.append(c)
          // A ']' right after '[' or '[^' is a literal, not the end.
          if (i + 1 < pattern.length && pattern.charAt(i + 1) == '^') { out.append('^'); i += 1 }
          if (i + 1 < pattern.length && pattern.charAt(i + 1) == ']') { out.append(']'); i += 1 }
        } else {
          if (c == ']' && inClass) inClass = false
          out.append(c)
        }
        i += 1
      }
    }
    out.toString
  }
}
