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

package org.apache.texera.amber.operator.udf

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.util.JSONUtils.objectMapper

import _root_.java.util.Locale
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Language-neutral metadata contract. Native value conversion belongs to each runtime. */
object UiUdfParameterContract {
  case class ParameterType(
      attributeType: AttributeType,
      token: String,
      aliases: List[String],
      requiresValue: Boolean
  )

  private val contract = Using.resource(
    getClass.getResourceAsStream("/udf/ui-parameter-types.json")
  )(objectMapper.readTree)
  val types: List[ParameterType] = contract
    .path("types")
    .elements()
    .asScala
    .map { definition =>
      ParameterType(
        AttributeType.valueOf(definition.path("attributeType").asText().toUpperCase(Locale.ROOT)),
        definition.path("token").asText(),
        definition.path("aliases").elements().asScala.map(_.asText()).toList,
        definition.path("requiresValue").asBoolean()
      )
    }
    .toList
  val typesByToken: Map[String, AttributeType] =
    types.flatMap(t => t.aliases.map(_ -> t.attributeType)).toMap
  val resourceInputTypes: Map[String, String] = contract
    .path("resources")
    .fields()
    .asScala
    .map(entry => entry.getKey -> entry.getValue.asText())
    .toMap

  def validate(uiParameters: List[UiUDFParameter]): Unit = {
    val names = Option(uiParameters).getOrElse(Nil).map { parameter =>
      val attribute = Option(parameter)
        .flatMap(p => Option(p.attribute))
        .getOrElse(throw new RuntimeException("UiParameter attribute is required."))
      if (attribute.getName.trim.isEmpty)
        throw new RuntimeException("UiParameter name is required.")
      val definition = types
        .find(_.attributeType == attribute.getType)
        .getOrElse(
          throw new RuntimeException(
            s"UiParameter type '${attribute.getType.name()}' is not supported. " +
              "Use string, integer, long, double, boolean, or timestamp instead."
          )
        )
      if (definition.requiresValue && Option(parameter.value).forall(_.trim.isEmpty))
        throw new RuntimeException(
          s"UiParameter '${attribute.getName}' requires a value for type '${attribute.getType.name()}'."
        )
      if (parameter.value == null)
        throw new RuntimeException(s"UiParameter '${attribute.getName}' value must be a string.")
      val resource = Option(parameter.inputType).getOrElse("")
      if (resource.nonEmpty && !resourceInputTypes.values.toSet.contains(resource))
        throw new RuntimeException(s"Unknown UiParameter resource '$resource'.")
      if (resource.nonEmpty && attribute.getType != AttributeType.STRING)
        throw new RuntimeException("Resource UiParameters must have type STRING.")
      attribute.getName
    }
    names
      .groupBy(identity)
      .collectFirst { case (name, copies) if copies.size > 1 => name }
      .foreach { name =>
        throw new RuntimeException(s"UiParameter name '$name' is declared more than once.")
      }
  }
}
