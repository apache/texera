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

package org.apache.texera.amber.operator.udf.java

import com.sun.source.tree._
import com.sun.source.util.{JavacTask, Trees}
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.udf.{UiUDFParameter, UiUdfParameterContract}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, ResolverStyle}
import java.time.temporal.ChronoField
import java.util.regex.Pattern
import java.util.{Base64, Collections, Locale}
import javax.lang.model.element.Modifier
import javax.tools._
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

/** Uses javac's source tree to initialize annotated fields without executing user code. */
object JavaUdfUiParameterInjector {
  private val annotationName = "org.apache.texera.amber.operator.udf.java.UiParameter"
  private val nativeTypes = UiUdfParameterContract.javaTypes.flatMap {
    case (kind, names) => names.map(_ -> kind)
  }
  private lazy val decimalPattern =
    Pattern.compile("[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
  private lazy val compactOffsetPattern = Pattern.compile("([+-][0-9]{2})([0-9]{2})$")
  private val timestampFormat = new DateTimeFormatterBuilder()
    .append(DateTimeFormatter.ISO_LOCAL_DATE)
    .optionalStart()
    .appendLiteral('T')
    .appendPattern("HH")
    .optionalStart()
    .appendPattern(":mm")
    .optionalStart()
    .appendPattern(":ss")
    .optionalStart()
    .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
    .optionalEnd()
    .optionalEnd()
    .optionalEnd()
    .optionalEnd()
    .optionalStart()
    .appendOffset("+HH:MM", "Z")
    .optionalEnd()
    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
    .toFormatter(Locale.ROOT)
    .withResolverStyle(ResolverStyle.STRICT)

  private def refuse(message: String): Nothing = throw new RuntimeException(s"UiParameter $message")
  private def stringExpression(text: String): String = {
    val encoded = Base64.getEncoder.encodeToString(text.getBytes(StandardCharsets.UTF_8))
    s"""new String(java.util.Base64.getDecoder().decode("$encoded"), java.nio.charset.StandardCharsets.UTF_8)"""
  }
  private def number(raw: String): Double =
    raw.toLowerCase(Locale.ROOT) match {
      case "nan" | "+nan" | "-nan"                   => Double.NaN
      case "inf" | "+inf" | "infinity" | "+infinity" => Double.PositiveInfinity
      case "-inf" | "-infinity"                      => Double.NegativeInfinity
      case value if decimalPattern.matcher(value).matches() =>
        value.toDouble
      case _ => refuse("requires a decimal numeric value.")
    }
  private def initializer(p: UiUDFParameter): String = {
    val raw = p.value.trim
    Try {
      p.attribute.getType match {
        case AttributeType.STRING  => stringExpression(p.value)
        case AttributeType.INTEGER => raw.toInt.toString
        case AttributeType.LONG    => s"${raw.toLong}L"
        case AttributeType.DOUBLE =>
          s"java.lang.Double.longBitsToDouble(${java.lang.Double.doubleToLongBits(number(raw))}L)"
        case AttributeType.BOOLEAN =>
          (raw.toLowerCase(Locale.ROOT) match {
            case "true"  => true
            case "false" => false
            case _       => number(raw) != 0d
          }).toString
        case AttributeType.TIMESTAMP =>
          val normalized = compactOffsetPattern.matcher(raw.replace(' ', 'T')).replaceFirst("$1:$2")
          val parsed = timestampFormat.parse(normalized)
          val offset =
            if (parsed.isSupported(ChronoField.OFFSET_SECONDS)) ZoneOffset.from(parsed)
            else ZoneOffset.UTC
          val instant = LocalDateTime.from(parsed).toInstant(offset)
          s"java.sql.Timestamp.from(java.time.Instant.parse(${stringExpression(instant.toString)}))"
        case _ => refuse("type is not supported.")
      }
    }.recover {
      case e: Exception =>
        throw new RuntimeException(
          s"UiParameter '${p.attribute.getName}' requires a valid ${p.attribute.getType.getName} value.",
          e
        )
    }.get
  }

  def inject(code: String, uiParameters: List[UiUDFParameter]): String = {
    val parameters = Option(uiParameters).getOrElse(Nil)
    UiUdfParameterContract.validate(parameters)
    val source = Option(code).getOrElse("")
    if (parameters.isEmpty && !source.contains("UiParameter")) return source
    val compiler = ToolProvider.getSystemJavaCompiler
    if (compiler == null) {
      if (parameters.isEmpty) return source
      refuse("declarations require a JDK with javac on the workflow compiling service.")
    }
    Javac.inject(source, parameters, compiler)
  }

  // Keep javac's tree classes unloaded on JRE hosts, even for a bare template's comment.
  private object Javac {
    private val sourceUri = URI.create("string:///JavaUDFOpExec.java")
    private val options = Collections.singletonList("-proc:none")

    def inject(source: String, parameters: List[UiUDFParameter], compiler: JavaCompiler): String = {
      val diagnostics = new DiagnosticCollector[JavaFileObject]()
      Using.resource(
        compiler.getStandardFileManager(diagnostics, Locale.ROOT, StandardCharsets.UTF_8)
      ) { manager =>
        val file = new SimpleJavaFileObject(sourceUri, JavaFileObject.Kind.SOURCE) {
          override def getCharContent(ignoreEncodingErrors: Boolean): CharSequence = source
        }
        val task = compiler
          .getTask(null, manager, diagnostics, options, null, Collections.singletonList(file))
          .asInstanceOf[JavacTask]
        val unit = task.parse().iterator().next()
        if (diagnostics.getDiagnostics.asScala.exists(_.getKind == Diagnostic.Kind.ERROR))
          refuse("declarations require valid Java syntax.")
        val classes = unit.getTypeDecls.asScala.collect {
          case cls: ClassTree if cls.getSimpleName.toString == "JavaUDFOpExec" => cls
        }
        if (classes.size != 1) refuse("declarations require one JavaUDFOpExec class.")
        val positions = Trees.instance(task).getSourcePositions
        val fields = classes.head.getMembers.asScala.collect { case field: VariableTree => field }
        val supplied = parameters.iterator.map(p => p.attribute.getName -> p).toMap
        val used = scala.collection.mutable.Set.empty[String]
        val edits = fields.flatMap { field =>
          val annotations =
            field.getModifiers.getAnnotations.iterator().asScala.filter { annotation =>
              val name = annotation.getAnnotationType.toString
              name == "UiParameter" || name == annotationName
            }
          if (!annotations.hasNext) None
          else {
            val annotation = annotations.next()
            if (annotations.hasNext) refuse("must be declared once per field.")
            val args = annotation.getArguments
            val values = args
              .iterator()
              .asScala
              .map {
                case argument: AssignmentTree =>
                  argument.getVariable.toString -> argument.getExpression
                case argument => "value" -> argument
              }
              .map {
                case (key, tree) =>
                  tree match {
                    case literal: LiteralTree if literal.getValue.isInstanceOf[String] =>
                      key -> literal.getValue.toString
                    case _ => refuse("annotation arguments must be string literals.")
                  }
              }
              .toMap
            if (
              values.size != args.size() || values.keysIterator
                .exists(key => key != "value" && key != "resource")
            )
              refuse("annotation arguments are invalid.")
            val name = values.getOrElse("value", "").trim
            if (used(name)) refuse(s"name '$name' is declared more than once.")
            used += name
            // javac includes the comma in the end position of a non-final declarator.
            val end = positions.getEndPosition(unit, field).toInt
            if (
              field.getModifiers.getFlags.contains(
                Modifier.STATIC
              ) || field.getInitializer != null || end <= 0 || source.charAt(end - 1) != ';'
            )
              refuse("requires a single uninitialized instance field per declaration.")
            val kind =
              nativeTypes.getOrElse(field.getType.toString, refuse("field type is not supported."))
            val parameter = supplied.getOrElse(name, refuse(s"'$name' has no saved value."))
            if (
              parameter.attribute.getType != kind || Option(parameter.inputType).getOrElse(
                ""
              ) != values.getOrElse("resource", "")
            )
              refuse(s"'$name' metadata does not match its field declaration.")
            Some((end - 1, initializer(parameter)))
          }
        }
        if (used != supplied.keySet) refuse("saved values do not match the declared fields.")
        if (edits.isEmpty) source
        else {
          val result = new java.lang.StringBuilder(source.length)
          var start = 0
          edits.sortBy(_._1).foreach {
            case (offset, value) =>
              result.append(source, start, offset).append(" = ").append(value)
              start = offset
          }
          result.append(source, start, source.length).toString
        }
      }
    }
  }
}
