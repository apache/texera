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
package org.apache.texera.amber.operator.udf.python

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType}
import org.apache.texera.amber.pybuilder.PythonLexerUtils.updateTripleQuotedStringState
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext

import scala.util.matching.Regex

/**
  * Injects the reserved UI-parameter hook into user-written Python UDF code.
  *
  * Operator descriptors should call this after loading saved [[UiUDFParameter]] values and before sending Python source
  * to runtime execution. The injected hook returns decoded parameter names and values that Python runtime support reads
  * before the user's `open()` method runs.
  */
object PythonUdfUiParameterInjector {

  private val InjectedUiParametersHookMethodName = "_texera_injected_ui_parameters"
  private val InjectedUiParametersHookMethodHeader =
    s"def $InjectedUiParametersHookMethodName(self) -> Dict[str, Any]:"
  private val UnsupportedUiParameterTypes = Set(AttributeType.BINARY, AttributeType.LARGE_BINARY)

  // Keep supported user-facing UDF class names in sync with the frontend parser.
  private val SupportedPythonUdfClassHeaderRegex: Regex =
    """(?m)^([ \t]*)class\s+(ProcessTupleOperator|ProcessBatchOperator|ProcessTableOperator|GenerateOperator)\s*\([^)]*\)\s*:\s*(?:#.*)?$""".r

  private def validate(uiParameters: List[UiUDFParameter]): Unit = {
    val attributes = uiParameters.map(parameterAttribute)
    attributes.foreach(validateSupportedType)

    attributes
      .groupBy(_.getName)
      .collectFirst {
        case (parameterName, matchingAttributes) if matchingAttributes.size > 1 => parameterName
      }
      .foreach { duplicateName =>
        throw new RuntimeException(s"UiParameter name '$duplicateName' is declared more than once.")
      }
  }

  private def parameterAttribute(parameter: UiUDFParameter): Attribute =
    Option(parameter).flatMap(parameter => Option(parameter.attribute)).getOrElse {
      throw new RuntimeException("UiParameter attribute is required.")
    }

  private def validateSupportedType(attribute: Attribute): Unit = {
    if (UnsupportedUiParameterTypes.contains(attribute.getType)) {
      throw new RuntimeException(
        s"UiParameter type '${attribute.getType.name()}' is not supported. " +
          "Use string, integer, long, double, boolean, or timestamp instead."
      )
    }
  }

  private def buildInjectedParameterEntry(parameter: UiUDFParameter): PythonTemplateBuilder = {
    pyb"${parameter.attribute.getName}: ${parameter.value}"
  }

  private def buildInjectedParametersMap(
      uiParameters: List[UiUDFParameter]
  ): PythonTemplateBuilder = {
    val entries = uiParameters.map(buildInjectedParameterEntry)
    entries.reduceOption((acc, entry) => acc + pyb", " + entry).getOrElse(pyb"")
  }

  private def buildInjectedHookMethod(uiParameters: List[UiUDFParameter]): String = {
    val injectedParametersMap = buildInjectedParametersMap(uiParameters)

    (pyb"""|# Follow-up runtime support exports Dict/Any and defines the base hook that @overrides targets.
           |@overrides
           |$InjectedUiParametersHookMethodHeader
           |    return {""" +
      injectedParametersMap +
      pyb"""}
           |""").encode
  }

  private def indentBlock(block: String, indent: String): String = {
    block
      .split("\n", -1)
      .map { line =>
        if (line.nonEmpty) indent + line else line
      }
      .mkString("\n")
  }

  private def lineEndIndex(text: String, from: Int): Int = {
    val lineEnd = text.indexOf('\n', from)
    if (lineEnd < 0) text.length else lineEnd
  }

  private def detectClassBlockEnd(code: String, classHeaderStart: Int, classIndent: String): Int = {
    val classLineEnd = lineEndIndex(code, classHeaderStart)
    var lineStart = if (classLineEnd < code.length) classLineEnd + 1 else code.length
    var tripleQuotedStringDelimiter: Option[String] = None

    while (lineStart < code.length) {
      val lineEnd = lineEndIndex(code, lineStart)
      val line = code.substring(lineStart, lineEnd)

      val trimmed = line.trim
      val isBlank = trimmed.isEmpty

      val currentIndentLen = line.segmentLength(ch => ch == ' ' || ch == '\t')
      val classIndentLen = classIndent.length

      if (tripleQuotedStringDelimiter.isEmpty && !isBlank && currentIndentLen <= classIndentLen) {
        return lineStart
      }

      tripleQuotedStringDelimiter = updateTripleQuotedStringState(line, tripleQuotedStringDelimiter)

      lineStart = if (lineEnd < code.length) lineEnd + 1 else code.length
    }

    code.length
  }

  private def containsReservedHook(classBlock: String): Boolean = {
    val hookRegex =
      ("""(?m)^[ \t]+def\s+""" + Regex.quote(InjectedUiParametersHookMethodName) + """\s*\(""").r
    hookRegex.findFirstIn(classBlock).isDefined
  }

  private def injectHookIntoUserClass(userCode: String, hookMethod: String): String = {
    val classHeaderMatch =
      SupportedPythonUdfClassHeaderRegex.findFirstMatchIn(userCode).getOrElse {
        throw new RuntimeException(
          "UiParameters were provided, but no supported Python UDF class was found. " +
            "Use one of ProcessTupleOperator, ProcessBatchOperator, ProcessTableOperator, or GenerateOperator."
        )
      }

    val classHeaderStart = classHeaderMatch.start
    val classIndent = classHeaderMatch.group(1)
    val classBlockEnd = detectClassBlockEnd(userCode, classHeaderStart, classIndent)

    val classBlock = userCode.substring(classHeaderStart, classBlockEnd)

    if (containsReservedHook(classBlock)) {
      throw new RuntimeException(
        s"Reserved method '$InjectedUiParametersHookMethodName' is already defined in the UDF class. Please rename your method."
      )
    }

    val bodyIndent = inferClassBodyIndent(classBlock, classIndent).getOrElse(classIndent + "    ")
    val indentedHook = indentBlock(
      (if (classBlock.endsWith("\n")) "" else "\n") + hookMethod.trim + "\n",
      bodyIndent
    )

    userCode.substring(0, classBlockEnd) +
      indentedHook +
      userCode.substring(classBlockEnd)
  }

  private def inferClassBodyIndent(classBlock: String, classIndent: String): Option[String] = {
    val lines = classBlock.split("\n", -1).toList.drop(1)

    lines.collectFirst {
      case line if line.trim.nonEmpty =>
        val leading = line.takeWhile(ch => ch == ' ' || ch == '\t')
        if (leading.length > classIndent.length) leading else classIndent + "    "
    }
  }

  /**
    * Returns Python code with the UI-parameter hook injected into the supported UDF class.
    *
    * If `uiParameters` is empty, the code is returned unchanged. Throws [[RuntimeException]] when parameter metadata is
    * invalid, the user already defines the reserved hook method, or parameters are provided for an unsupported class.
    */
  def inject(code: String, uiParameters: List[UiUDFParameter]): String = {
    val parameters = Option(uiParameters).getOrElse(List.empty)
    validate(parameters)

    val userCode = Option(code).getOrElse("")

    if (parameters.isEmpty) {
      return userCode
    }

    val hookMethod = buildInjectedHookMethod(parameters)
    injectHookIntoUserClass(userCode, hookMethod)
  }
}
