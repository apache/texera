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
package org.apache.texera.common.compiler

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.networknt.schema.{JsonSchema, JsonSchemaFactory, SpecVersion}
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.state.StateReferencing.referencedVariable
import org.apache.texera.amber.core.workflowruntimestate.WorkflowFatalError
import org.apache.texera.amber.operator.metadata.OperatorMetadataGenerator
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Validate effective properties with the same schemas used by Texera's property editor. */
private[compiler] object MacroValidation {
  private lazy val metadata = OperatorMetadataGenerator.allOperatorMetadata.operators
    .map(op => op.operatorType -> op)
    .toMap
  private val factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
  private val schemas = TrieMap.empty[String, JsonSchema]

  def operatorName(operatorType: String): String =
    metadata.get(operatorType).map(_.additionalMetadata.userFriendlyName).getOrElse(operatorType)

  private def path(expansion: MacroExpansion, id: String): String =
    expansion.names.getOrElse(id, expansion.paths.getOrElse(id, List(id))).mkString(" / ")

  class ValidationErrors(val errors: List[(OperatorIdentity, Throwable)], expansion: MacroExpansion)
      extends IllegalArgumentException(
        errors
          .map { case (id, error) => s"${path(expansion, id.id)}: ${error.getMessage}" }
          .mkString("\n")
      )

  def validate(
      expansion: MacroExpansion,
      descriptors: Map[String, Try[LogicalOp]],
      propertiesOnly: Boolean = false
  ): Unit = {
    lazy val incoming = expansion.content
      .path("links")
      .elements()
      .asScala
      .toList
      .groupBy(link =>
        (
          link.path("target").path("operatorID").asText(),
          link.path("target").path("portID").asText()
        )
      )
    val errors = expansion.content.path("operators").elements().asScala.toList.flatMap { op =>
      val id = op.path("operatorID").asText()
      val kind = op.path("operatorType").asText()
      val messages = metadata.get(kind).fold(List(s"Unknown operator type: $kind")) { meta =>
        val descriptor = descriptors(id).toOption
        val schema = schemas.getOrElseUpdate(kind, factory.getSchema(meta.jsonSchema))
        // Declared defaults use descriptor initializers; explicit nulls and invalid types stay intact.
        val effective = descriptor.fold(op.path("operatorProperties")) { descriptor =>
          objectMapper
            .valueToTree[ObjectNode](descriptor)
            .retain(
              meta.jsonSchema
                .path("properties")
                .fields()
                .asScala
                .filter(_.getValue.has("default"))
                .map(_.getKey)
                .toList
                .asJava
            )
            .setAll[JsonNode](op.path("operatorProperties").asInstanceOf[ObjectNode])
        }
        // Typed loop references are checked by the existing state-reference compiler after parsing.
        val properties = schema
          .validate(effective)
          .asScala
          .toList
          .filterNot { error =>
            error.getType == "type" && error.getInstanceNode.isTextual &&
            referencedVariable(error.getInstanceNode.asText()).nonEmpty &&
            Set("integer", "number", "boolean").contains(error.getSchemaNode.asText())
          }
          .map(_.getMessage)
        if (propertiesOnly) {
          if (properties.nonEmpty) properties
          else descriptors(id).failed.toOption.toList.map(_.getMessage)
        } else {
          val info = descriptor.map(_.operatorInfo).getOrElse(meta.additionalMetadata)
          val inputs = op
            .path("inputPorts")
            .elements()
            .asScala
            .zipWithIndex
            .flatMap {
              case (port, index) =>
                val portID = port.path("portID").asText()
                val count = incoming.getOrElse((id, portID), Nil).size
                val single = info.inputPorts.lift(index).exists(_.disallowMultiLinks)
                Option.when(count == 0 || (single && count != 1))(
                  s"Input '${port.path("displayName").asText(portID)}' ($portID) requires ${if (single) "1 input"
                  else "at least 1 input"}, has $count"
                )
            }
            .toList
          val declarations = Option
            .when(
              !info.dynamicInputPorts && op.path("inputPorts").size() != info.inputPorts.size
            )(
              s"Expected ${info.inputPorts.size} input ports, found ${op.path("inputPorts").size()}"
            )
            .toList
          properties ++ inputs ++ declarations
        }
      }
      Option.when(messages.nonEmpty)(
        OperatorIdentity(id) -> new IllegalArgumentException(
          messages.distinct.sorted.mkString("; ")
        )
      )
    }
    if (errors.nonEmpty) throw new ValidationErrors(errors, expansion)
  }

  /** Keep leaf errors for inspectors and aggregate all descendants on each enclosing macro. */
  def propagate(
      expansion: MacroExpansion,
      errors: Map[OperatorIdentity, WorkflowFatalError]
  ): Map[OperatorIdentity, WorkflowFatalError] = {
    val named = errors.map {
      case (id, error) =>
        id -> error.copy(message = s"${path(expansion, id.id)}: ${error.message}")
    }
    val ancestors = named.toList
      .flatMap {
        case (id, error) =>
          val ids = expansion.paths.getOrElse(id.id, Nil)
          ids.indices
            .dropRight(1)
            .map(index =>
              OperatorIdentity(MacroExpansion.operatorID(ids.take(index), ids(index))) -> error
            )
      }
      .groupBy(_._1)
      .map {
        case (id, children) =>
          id -> children.head._2.copy(
            operatorId = id.id,
            message = children.map(_._2.message).distinct.sorted.mkString("\n")
          )
      }
    named ++ ancestors
  }
}
