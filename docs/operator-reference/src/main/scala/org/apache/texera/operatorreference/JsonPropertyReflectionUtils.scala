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

package org.apache.texera.operatorreference

import com.fasterxml.jackson.annotation.{
  JsonCreator,
  JsonIgnore,
  JsonIgnoreProperties,
  JsonProperty,
  JsonPropertyDescription,
  JsonValue
}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaDescription,
  JsonSchemaInject,
  JsonSchemaTitle
}
import org.apache.texera.amber.core.workflow.OutputPort.OutputMode
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorMetadataGenerator
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.{
  HyperParameters,
  ParamClass
}
import org.apache.texera.amber.operator.metadata.annotations.HideAnnotation

import java.lang.annotation.Annotation
import java.lang.reflect.{Field, Modifier, ParameterizedType}
import javax.validation.constraints.{NotNull, Size}
import scala.jdk.CollectionConverters._
import scala.util.Try

private[operatorreference] case class DocProperty(
    displayName: String,
    required: Boolean,
    dataType: String,
    defaultValue: String,
    description: String,
    children: List[DocProperty] = Nil,
    codeTemplate: Option[String] = None
)

private[operatorreference] case class EnumParam(
    name: String,
    paramType: String,
    defaultValue: String,
    constraint: String
)

object JsonPropertyReflectionUtils {

  private case class HideRule(target: String, hideType: String, expected: String)

  // The section headings of the hand-written output-modes.md page.
  private val OutputModeLabels: Map[OutputMode, String] = Map(
    OutputMode.SET_SNAPSHOT -> "Set Snapshot",
    OutputMode.SET_DELTA -> "Delta Updates",
    OutputMode.SINGLE_SNAPSHOT -> "Single Snapshot"
  )
  private val HideEquals = HideAnnotation.Type.`equals`
  private val HideRegex = HideAnnotation.Type.regex
  private val mapper = new ObjectMapper()

  def generatePropertiesTable(clazz: Class[_], rootPath: String): Option[String] = {
    val properties = extractProperties(clazz, rootPath)
    if (properties.isEmpty) return None

    def escapeCell(s: String) = s.replace("\n", " ").replace("\r", " ").replace("|", "\\|")
    def renderRows(p: DocProperty, depth: Int): List[String] = {
      val indent = if (depth == 0) "" else "&nbsp;&nbsp;" * (depth - 1) + "↳ "
      val requirement = if (p.required) "✓" else ""
      val row =
        s"| ${escapeCell(indent + p.displayName)} | $requirement | ${escapeCell(p.dataType)} | " +
          s"${escapeCell(p.defaultValue)} | ${wrap(escapeCell(p.description), 50, " ")} |"
      row :: p.children.flatMap(renderRows(_, depth + 1))
    }
    val table =
      ("| Property | Requirement | Type | Default | Description |" ::
        "|----------|-------------|------|---------|-------------|" :: properties.flatMap(
        renderRows(_, 0)
      ))
        .mkString("\n")

    val templates = properties.flatMap(p => p.codeTemplate.map(t => (p.displayName, t)))
    if (templates.isEmpty) Some(table)
    else {
      val language = codeLanguage(clazz)
      val sections = templates.map {
        case (name, template) => s"**$name**\n\n```$language\n$template\n```"
      }
      Some(table + "\n\n#### Default Code Template\n\n" + sections.mkString("\n\n"))
    }
  }

  def generateOutputSection(clazz: Class[_], rootPath: String): String = {
    val ports = createInstance(clazz).collect { case op: LogicalOp => op.operatorInfo.outputPorts }
    if (ports.isEmpty) return "*Output ports unavailable: the descriptor could not be instantiated*"
    if (ports.get.isEmpty) return "*No output ports*"

    val rows = ports.get.zipWithIndex.map {
      case (port, idx) =>
        val label = OutputModeLabels.getOrElse(port.mode, port.mode.name)
        val anchor = label.toLowerCase.replace(' ', '-')
        s"| $idx | [$label](${rootPath}output-modes/#$anchor) |"
    }
    ("### Output Ports\n\n| Port | Mode |\n|------|------|" :: rows).mkString("\n")
  }

  def getEnumClass(operatorClass: Class[_]): Option[Class[_]] =
    operatorClass.getGenericSuperclass match {
      case pt: ParameterizedType =>
        pt.getActualTypeArguments.headOption.collect { case c: Class[_] if c.isEnum => c }
      case _ => None
    }

  def extractEnumMetadata(enumClass: Class[_]): List[EnumParam] =
    enumConstants(enumClass).collect {
      case p: ParamClass =>
        // getType is the Python converter the generated code applies to the chosen value.
        val paramType = p.getType
        val constraint =
          if (p.getAllowedValues.nonEmpty) p.getAllowedValues.mkString(", ")
          else if (p.getMinimum.nonEmpty) p.getMinimum
          else "-"
        EnumParam(
          p.getName,
          paramType,
          Some(p.getSampleValue).filter(_.nonEmpty).getOrElse("-"),
          constraint
        )
    }

  private def extractProperties(clazz: Class[_], rootPath: String): List[DocProperty] = {
    val instance = createInstance(clazz)
    val fields = documentableFields(clazz)
    val byJsonName = fields.map(f => jsonName(f) -> f).toMap

    def toggleParent(field: Field): Option[Field] =
      fields
        .find(t => toggledFields(t).exists(n => n == field.getName || n == jsonName(field)))
        .orElse {
          hideRule(field).collect {
            case HideRule(target, HideEquals, _) =>
              byJsonName.get(target).filter { t =>
                Set[Class[_]](classOf[Boolean], classOf[java.lang.Boolean])
                  .exists(b => t.getType == b || contentAs(t).contains(b))
              }
          }.flatten
        }

    def toProperty(field: Field): DocProperty = {
      val (dataType, children) = dataTypeAndChildren(field, clazz, rootPath)
      val template = codeTemplate(field)
      val hideNote = hideRule(field).map {
        case HideRule(target, HideRegex, expected) =>
          s"Hidden when ${targetLabel(target, byJsonName)} matches `$expected`"
        case HideRule(target, _, expected) =>
          s"Hidden when ${targetLabel(target, byJsonName)} is `$expected`"
      }
      DocProperty(
        displayName(field),
        isRequired(field),
        dataType,
        if (template.isDefined) "`See template below`" else defaultValue(field, instance),
        (description(field, clazz) ++ hideNote)
          .reduceOption((a, b) => if (a.endsWith(".") || a.endsWith(")")) s"$a $b" else s"$a. $b")
          .getOrElse(""),
        children,
        template
      )
    }

    fields.filter(f => toggleParent(f).isEmpty).map { field =>
      val property = toProperty(field)
      val toggled = fields.filter(g => toggleParent(g).contains(field)).map(toProperty)
      property.copy(children = property.children ++ toggled)
    }
  }

  private def targetLabel(target: String, byJsonName: Map[String, Field]): String =
    s"`${byJsonName.get(target).map(displayName).getOrElse(target)}`"

  private def documentableFields(clazz: Class[_]): List[Field] = {
    val schemaOrder = schemaPropertyOrder(clazz)
    val chain = Iterator
      .iterate[Class[_]](clazz)(_.getSuperclass)
      .takeWhile(c => c != null && c != classOf[Object] && c != classOf[LogicalOp])
      .toList
      .reverse
    val ignored = chain
      .flatMap(c => Option(c.getAnnotation(classOf[JsonIgnoreProperties])))
      .flatMap(_.value())
      .toSet
    chain
      .flatMap(_.getDeclaredFields)
      .filterNot(f => f.isSynthetic || Modifier.isStatic(f.getModifiers) || f.getName.contains("$"))
      .filterNot(f =>
        ignored.contains(f.getName) || findAnnotation(f, classOf[JsonIgnore]).isDefined
      )
      .filter { f =>
        findAnnotation(f, classOf[JsonProperty]).isDefined ||
        findAnnotation(f, classOf[JsonSchemaTitle]).isDefined ||
        findAnnotation(f, classOf[JsonPropertyDescription]).isDefined ||
        findAnnotation(f, classOf[JsonSchemaDescription]).isDefined
      }
      .groupBy(_.getName)
      .values
      .map(_.last)
      .toList
      .sortBy(f => schemaOrder.indexOf(jsonName(f)))
  }

  // The property panel lists properties in the JSON schema's order, so the pages do too.
  private def schemaPropertyOrder(clazz: Class[_]): List[String] =
    Try(OperatorMetadataGenerator.jsonSchemaGenerator.generateJsonSchema(clazz)).toOption
      .map(_.path("properties").fieldNames().asScala.toList)
      .getOrElse(Nil)

  private def jsonName(field: Field): String =
    findAnnotation(field, classOf[JsonProperty])
      .map(_.value())
      .filter(_.nonEmpty)
      .getOrElse(field.getName)

  private def displayName(field: Field): String =
    findAnnotation(field, classOf[JsonSchemaTitle])
      .map(_.value())
      .filter(_.nonEmpty)
      .getOrElse(toTitleCase(jsonName(field)))

  private def isRequired(field: Field): Boolean =
    findAnnotation(field, classOf[JsonProperty]).exists(_.required()) ||
      findAnnotation(field, classOf[NotNull]).isDefined

  private def defaultValue(field: Field, instance: Option[AnyRef]): String =
    findAnnotation(field, classOf[JsonProperty])
      .map(_.defaultValue())
      .filter(v => v.nonEmpty && v != "\u0000")
      .orElse(instance.flatMap { inst =>
        field.setAccessible(true)
        Try(field.get(inst)).toOption.flatMap(formatDefault)
      })
      .getOrElse("-")

  private def formatDefault(value: Any): Option[String] =
    value match {
      case null | None | "" | Nil                    => None
      case Some(inner)                               => formatDefault(inner)
      case xs: scala.collection.Iterable[_]          => Some(s"[${xs.size} items]")
      case xs: java.util.Collection[_] if xs.isEmpty => None
      case xs: java.util.Collection[_]               => Some(s"[${xs.size} items]")
      case other                                     => Some(other.toString)
    }

  private def description(field: Field, owner: Class[_]): Option[String] = {
    val base = getterAnnotation(field, classOf[JsonPropertyDescription], owner)
      .orElse(findAnnotation(field, classOf[JsonPropertyDescription]))
      .map(_.value())
      .orElse(findAnnotation(field, classOf[JsonSchemaDescription]).map(_.value()))
      .filter(_.nonEmpty)
      .map(s => s"${s.head.toUpper}${s.tail}".stripSuffix("."))
    val constraints = findAnnotation(field, classOf[Size]).toList.flatMap { size =>
      Option(size.min()).filter(_ > 0).map(m => s"min: $m") ++
        Option(size.max()).filter(_ < Int.MaxValue).map(m => s"max: $m") ++
        Option(size.message()).filter(m => m.nonEmpty && !m.startsWith("{"))
    }
    (base, constraints) match {
      case (Some(b), Nil) => Some(b)
      case (Some(b), cs)  => Some(s"$b (${cs.mkString(", ")})")
      case (None, Nil)    => None
      case (None, cs)     => Some(cs.mkString(", "))
    }
  }

  private def hideRule(field: Field): Option[HideRule] =
    findAnnotation(field, classOf[JsonSchemaInject]).flatMap { inject =>
      val strings = inject.strings().map(s => s.path() -> s.value()).toMap
      for {
        target <- strings.get(HideAnnotation.hideTarget)
        hideType <- strings.get(HideAnnotation.hideType)
        expected <- strings.get(HideAnnotation.hideExpectedValue)
      } yield HideRule(target, hideType, expected)
    }

  private def toggledFields(field: Field): List[String] =
    findAnnotation(field, classOf[JsonSchemaInject]).toList.flatMap { inject =>
      Try(mapper.readTree(inject.json()).get("toggleHidden")).toOption
        .filter(_ != null)
        .toList
        .flatMap(_.asScala.map(_.asText()))
    }

  // Erasure hides the element type of Option/List fields; descriptors name it with contentAs.
  private def contentAs(field: Field): Option[Class[_]] =
    findAnnotation(field, classOf[JsonDeserialize])
      .map(_.contentAs())
      .filter(c => c != classOf[Void] && c != classOf[Object])

  // The frontend opens the code editor for exactly this description.
  private def codeTemplate(field: Field): Option[String] =
    findAnnotation(field, classOf[JsonPropertyDescription])
      .filter(_.value().equalsIgnoreCase("Input your code here"))
      .flatMap(_ => findAnnotation(field, classOf[JsonProperty]))
      .map(_.defaultValue())
      .filter(_.nonEmpty)

  // Mirrors the frontend's CodeEditorComponent, which picks the language by operator type.
  private def codeLanguage(clazz: Class[_]): String = {
    val name = clazz.getSimpleName
    if (name.contains("Python")) "python" else if (name.startsWith("RUDF")) "r" else "java"
  }

  private def dataTypeAndChildren(
      field: Field,
      owner: Class[_],
      rootPath: String
  ): (String, List[DocProperty]) = {
    if (codeTemplate(field).isDefined) return (s"Code (${codeLanguage(owner)})", Nil)
    val accepted = attributeTypeRules(owner).getOrElse(jsonName(field), Nil)
    val (dataType, children) = plainTypeAndChildren(field, owner, rootPath)
    (if (accepted.isEmpty) dataType else s"$dataType (${accepted.mkString(", ")})", children)
  }

  private def plainTypeAndChildren(
      field: Field,
      owner: Class[_],
      rootPath: String
  ): (String, List[DocProperty]) = {

    val raw = field.getType
    val typeArgs = field.getGenericType match {
      case pt: ParameterizedType =>
        pt.getActualTypeArguments.toList.collect {
          case c: Class[_]          => c
          case p: ParameterizedType => p.getRawType.asInstanceOf[Class[_]]
        }
      case _ => Nil
    }
    val element = contentAs(field).orElse(typeArgs.lastOption.filter(_ != classOf[Object]))

    if (classOf[Option[_]].isAssignableFrom(raw)) {
      (element.map(simpleType).getOrElse("Object"), Nil)
    } else if (
      classOf[scala.collection.Iterable[_]].isAssignableFrom(raw) ||
      classOf[java.lang.Iterable[_]].isAssignableFrom(raw)
    ) {
      element match {
        case Some(c) if c == classOf[HyperParameters[_]] =>
          val link = getEnumClass(owner).map { e =>
            s"[${e.getSimpleName}](${rootPath}parameters/${parameterPageSlug(e)}/)"
          }
          (link.getOrElse("List<Parameter>"), Nil)
        case Some(c) if isNestedConfig(c) =>
          (s"List<${toTitleCase(c.getSimpleName)}>", extractProperties(c, rootPath))
        case Some(c) => (s"List<${simpleType(c)}>", Nil)
        case None    => ("List", Nil)
      }
    } else {
      (simpleType(raw), Nil)
    }
  }

  private def simpleType(c: Class[_]): String =
    if (c.isEnum) enumOptions(c)
    else
      c.getSimpleName match {
        case "int" | "Integer"     => "Integer"
        case "long" | "Long"       => "Long"
        case "double" | "Double"   => "Double"
        case "float" | "Float"     => "Float"
        case "boolean" | "Boolean" => "Boolean"
        case other                 => other
      }

  private def enumOptions(enumClass: Class[_]): String = {
    val jsonValue = enumClass.getMethods.find { m =>
      m.getAnnotation(classOf[JsonValue]) != null && m.getParameterCount == 0
    }
    val options = enumConstants(enumClass)
      .map(c => jsonValue.flatMap(m => Try(m.invoke(c).toString).toOption).getOrElse(c.toString))
      .filter(_.nonEmpty)
    wrap(options.mkString(", "), 40, ", ")
  }

  private def isNestedConfig(c: Class[_]): Boolean =
    !c.isEnum && !c.getName.startsWith("java.") && !c.getName.startsWith("scala.") &&
      documentableFields(c).nonEmpty

  private def attributeTypeRules(clazz: Class[_]): Map[String, List[String]] = {
    val injected = Iterator
      .iterate[Class[_]](clazz)(_.getSuperclass)
      .takeWhile(c => c != null && c != classOf[Object])
      .flatMap(c => Option(c.getAnnotation(classOf[JsonSchemaInject])))
      .map(_.json())
      .filter(_.nonEmpty)
      .toList
    injected.flatMap { json =>
      Try(mapper.readTree(json).get("attributeTypeRules")).toOption
        .filter(rules => rules != null && rules.isObject)
        .toList
        .flatMap { rules =>
          rules.properties().asScala.map { entry =>
            val value = entry.getValue
            val accepted =
              if (value.has("enum")) value.get("enum").asScala.map(_.asText()).toList
              else if (value.isTextual) List(value.asText())
              else Nil
            entry.getKey -> accepted
          }
        }
    }.toMap
  }

  def parameterPageSlug(enumClass: Class[_]): String =
    enumClass.getSimpleName
      .replaceAll("([A-Z]+)([A-Z][a-z])", "$1-$2")
      .replaceAll("([a-z\\d])([A-Z])", "$1-$2")
      .toLowerCase

  /**
    * Finds an annotation on a field, falling back to the matching `@JsonCreator` constructor
    * parameter, where annotations on Scala case-class fields land.
    */
  private def findAnnotation[T <: Annotation](field: Field, annotation: Class[T]): Option[T] =
    Option(field.getAnnotation(annotation)).orElse {
      field.getDeclaringClass.getDeclaredConstructors
        .find(_.getAnnotation(classOf[JsonCreator]) != null)
        .flatMap { ctor =>
          ctor.getParameters.find { p =>
            Option(p.getAnnotation(classOf[JsonProperty])).exists(_.value() == field.getName) ||
            p.getName == field.getName
          }
        }
        .flatMap(p => Option(p.getAnnotation(annotation)))
    }

  /** An annotation a subclass puts on the field's getter, which Jackson merges into the property. */
  private def getterAnnotation[T <: Annotation](
      field: Field,
      annotation: Class[T],
      owner: Class[_]
  ): Option[T] = {
    val getter = s"get${field.getName.head.toUpper}${field.getName.tail}"
    owner.getMethods
      .find(m => m.getName == getter && m.getParameterCount == 0)
      .flatMap(m => Option(m.getAnnotation(annotation)))
  }

  // Breaks a cell longer than `width` at the last separator before it, so it does not stretch
  // the table on the site.
  private def wrap(text: String, width: Int, separator: String): String = {
    val cut = if (text.length <= width) -1 else text.lastIndexOf(separator, width - 1)
    if (cut <= 0) text
    else
      s"${text.take(cut)}${separator.trim}<br>${wrap(text.drop(cut + separator.length), width, separator)}"
  }

  private def enumConstants(enumClass: Class[_]): List[AnyRef] =
    enumClass.getEnumConstants.asInstanceOf[Array[AnyRef]].toList

  private def createInstance(clazz: Class[_]): Option[AnyRef] =
    Try(clazz.getDeclaredConstructor().newInstance().asInstanceOf[AnyRef]).toOption

  private def toTitleCase(s: String): String =
    s.replaceAll("([A-Z]+)([A-Z][a-z])", "$1 $2")
      .replaceAll("([a-z\\d])([A-Z])", "$1 $2")
      .replace('_', ' ')
      .split("\\s+")
      .filter(_.nonEmpty)
      .map(w => s"${w.head.toUpper}${w.tail}")
      .mkString(" ")
}
