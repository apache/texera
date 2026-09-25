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

package org.apache.texera.amber.operator.unneststring

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{
  InputPort,
  OutputPort,
  PhysicalOp,
  PortIdentity,
  SchemaPropagationFunc
}
import org.apache.texera.amber.operator.{StandaloneCodeGenerator, StandaloneHelpers}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.operator.flatmap.FlatMapOpDesc
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  SampleColumn,
  UIWidget
}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.util.regex.{Pattern, PatternSyntaxException}

class UnnestStringOpDesc extends FlatMapOpDesc with StandaloneCodeGenerator {
  @JsonProperty(value = "Delimiter", required = true, defaultValue = ",")
  @JsonPropertyDescription("regular expression that separates the data")
  @JsonSchemaInject(json = UIWidget.UIWidgetRegexDelimiter)
  var delimiter: String = _

  @JsonProperty(value = "Attribute", required = true)
  @JsonPropertyDescription("column of the string to unnest")
  @AutofillAttributeName
  @SampleColumn("csv_list")
  var attribute: String = _

  @JsonProperty(value = "Result attribute", required = true, defaultValue = "unnestResult")
  @JsonPropertyDescription("column name of the unnest result")
  var resultAttribute: String = _

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Unnest String",
      operatorDescription =
        "Unnest the string values in the column separated by a delimiter to multiple values",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

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
          "org.apache.texera.amber.operator.unneststring.UnnestStringOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          validateDelimiter()
          val outputSchema = Option(resultAttribute)
            .filter(_.trim.nonEmpty)
            .map(attr => inputSchemas.values.head.add(attr, AttributeType.STRING))
            .getOrElse(throw new RuntimeException("Result attribute cannot be empty"))
          Map(operatorInfo.outputPorts.head.id -> outputSchema)
        })
      )
  }

  // Checked here, with Java's engine, so a bad pattern surfaces on the operator as a readable
  // message while editing instead of as an exception or an empty result once the workflow
  // runs. The picker checks the same things in the browser, but defers to this for syntax
  // JavaScript can't parse or reads differently (inline flags, \p{..}).
  private def validateDelimiter(): Unit = {
    val source = Option(delimiter).getOrElse("")
    if (source.isEmpty) {
      throw new RuntimeException("Delimiter cannot be empty")
    }
    val pattern =
      try Pattern.compile(source)
      catch {
        case e: PatternSyntaxException =>
          throw new RuntimeException(
            s"Delimiter is not a valid regular expression: ${e.getDescription}" +
              s" near index ${e.getIndex}"
          )
      }
    if (pattern.matcher("").find()) {
      throw new RuntimeException(
        "Delimiter matches an empty string, so it would split between every character"
      )
    }
    if (UnnestStringOpDesc.matchesEveryCharacter(pattern)) {
      val literal =
        if (source.length == 1) s""". To split on a literal "$source", use \\$source""" else ""
      // A value's line breaks survive a pattern that leaves them out, so say what is left.
      val whatIsLeft =
        if (UnnestStringOpDesc.matchesLineBreaks(pattern))
          "matches every character, so nothing would be left"
        else "matches every character except line breaks, so only line breaks would be left"
      throw new RuntimeException(s"Delimiter $whatIsLeft$literal")
    }
  }

  override def generateStandaloneCode(): String = generateStandaloneCode(Map.empty)

  override def generateStandaloneCode(inputSchemas: Map[PortIdentity, Schema]): String = {
    if (resultAttribute == null || resultAttribute.trim.isEmpty) {
      throw new RuntimeException("Result attribute cannot be empty")
    }
    // The JVM op uses Scala's `delimiter.r.split(...)`, so delimiter is a regex; it
    // and the two column names are rendered as escaped Python literals.
    val delim = pyStringLiteral(Option(delimiter).getOrElse(""))
    val resultLit = pyStringLiteral(resultAttribute)
    val attributeLit = pyStringLiteral(attribute)
    // What is split is the text the engine split, not whatever pandas made of the
    // column. See [[renderedAsText]].
    val declared = inputSchemas.values.headOption
      .flatMap(schema => scala.util.Try(schema.getAttribute(attribute)).toOption)
      .map(_.getType)
    val column = renderedAsText(s"out1df[$attributeLit]", declared)
    s"""# Nothing in the column unnests to nothing, the way the operator answers a null
       |# field with no rows at all. Dropped before the split rather than after: the
       |# rendering would turn the empty cell into text and unnest that.
       |out1df = in1df[in1df[$attributeLit].notna()].copy()
       |out1df[$resultLit] = $column.str.split($delim, regex=True)
       |out1df = out1df.explode($resultLit, ignore_index=True)
       |out1df = out1df[(out1df[$resultLit].notna()) & (out1df[$resultLit] != "")].reset_index(drop=True)""".stripMargin
  }

  override def standaloneHelpers(): Seq[String] = Seq(StandaloneHelpers.AttributeCasts)
}

object UnnestStringOpDesc {
  // Line terminators, which `.` leaves out. Mirrors LINE_BREAKS in the frontend's
  // delimiter-presets.ts.
  private val LineBreaks: Seq[Int] = Seq(0x0a, 0x0d, 0x85, 0x2028, 0x2029)

  private def matches(pattern: Pattern, code: Int): Boolean =
    pattern.matcher(String.valueOf(code.toChar)).find()

  /**
    * Whether the pattern matches every character of the Basic Multilingual Plane other than line
    * breaks (and lone surrogate halves, which are not characters on their own), each tested on
    * its own. Exact rather than a sample, and cheap: a pattern that leaves any character behind
    * stops at the first one it misses. Mirrors matchesEveryCharacter in the frontend's
    * delimiter-presets.ts.
    */
  private def matchesEveryCharacter(pattern: Pattern): Boolean =
    (0 to 0xffff).iterator
      .filterNot(code => LineBreaks.contains(code) || code.toChar.isSurrogate)
      .forall(matches(pattern, _))

  private def matchesLineBreaks(pattern: Pattern): Boolean =
    LineBreaks.forall(matches(pattern, _))
}
