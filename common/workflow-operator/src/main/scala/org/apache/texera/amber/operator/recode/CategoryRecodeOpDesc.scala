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

package org.apache.texera.amber.operator.recode

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{
  InputPort,
  OutputPort,
  PhysicalOp,
  SchemaPropagationFunc
}
import org.apache.texera.amber.operator.map.MapOpDesc
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import javax.validation.constraints.Size

class CategoryRecodeOpDesc extends MapOpDesc {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Source column")
  @JsonPropertyDescription(
    "String, integer, long, double or boolean column to recode. The source is preserved."
  )
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("New category column")
  @JsonPropertyDescription("Name of a new text column; must not duplicate an existing column.")
  var outputAttribute: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Value mappings")
  @Size(min = 1)
  @JsonPropertyDescription(
    "Exact value-to-category pairs. No trimming, case folding, ranges or pattern matching."
  )
  var mappings: List[CategoryMapping] = List.empty

  @JsonProperty(required = true, defaultValue = "Set null")
  @JsonSchemaTitle("Unmatched values")
  @JsonPropertyDescription(
    "How to handle non-null values absent from the mappings. Keep original converts them to text."
  )
  var unmatched: UnmatchedCategoryPolicy = UnmatchedCategoryPolicy.SET_NULL

  @JsonProperty
  @JsonSchemaTitle("Default category")
  @JsonPropertyDescription("Required only when Unmatched values is Use default.")
  var defaultCategory: String = _

  @JsonProperty
  @JsonSchemaTitle("Missing-value category (optional)")
  @JsonPropertyDescription(
    "Null inputs remain null unless this category is supplied. Independent of unmatched handling."
  )
  var missingCategory: String = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          classOf[CategoryRecodeOpExec].getName,
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas =>
          Map(operatorInfo.outputPorts.head.id -> compile(inputSchemas.values.head).outputSchema)
        )
      )

  private[recode] def compile(input: Schema): CompiledCategoryRecode = {
    require(attribute != null && input.containsAttribute(attribute), "Source column must exist.")
    require(
      outputAttribute != null && outputAttribute.trim.nonEmpty,
      "Output column must have a nonblank name."
    )
    require(!input.containsAttribute(outputAttribute), "Output column already exists.")
    val kind = input.getAttribute(attribute).getType
    require(
      Set(
        AttributeType.STRING,
        AttributeType.INTEGER,
        AttributeType.LONG,
        AttributeType.DOUBLE,
        AttributeType.BOOLEAN
      ).contains(kind),
      "Source column type must be string, integer, long, double or boolean."
    )
    require(unmatched != null, "Unmatched values policy is required.")
    require(
      unmatched != UnmatchedCategoryPolicy.USE_DEFAULT || defaultCategory != null,
      "Default category is required for Use default."
    )
    require(mappings != null && mappings.nonEmpty, "At least one mapping is required.")

    val compiled = mappings.foldLeft(Map.empty[Any, String]) { (result, rule) =>
      require(
        rule != null && rule.value != null && rule.category != null,
        "Each mapping requires a source value and category."
      )
      val key = CategoryRecodeOpDesc.parseKey(rule.value, kind)
      require(!result.contains(key), "Duplicate mapping for the same source value.")
      result.updated(key, rule.category)
    }
    CompiledCategoryRecode(
      input,
      input.add(outputAttribute, AttributeType.STRING),
      input.getIndex(attribute),
      compiled,
      unmatched,
      defaultCategory,
      missingCategory
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Category Recode",
      "Map exact column values to text categories while preserving the source column.",
      OperatorGroupConstants.CLEANING_GROUP,
      List(InputPort()),
      List(OutputPort())
    )
}

private[recode] object CategoryRecodeOpDesc {
  private val integerPattern = "[+-]?[0-9]+".r
  private val doublePattern = "[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?".r

  def parseKey(value: String, kind: AttributeType): Any = {
    def invalid(): Nothing =
      throw new IllegalArgumentException(s"Mapping value is not a valid $kind value.")

    try {
      kind match {
        case AttributeType.STRING                                   => value
        case AttributeType.INTEGER if integerPattern.matches(value) => value.toInt
        case AttributeType.LONG if integerPattern.matches(value)    => value.toLong
        case AttributeType.DOUBLE if doublePattern.matches(value) =>
          val number = value.toDouble
          // Reject overflow and underflow: neither should silently turn a code into another code.
          if (!java.lang.Double.isFinite(number) || (number == 0.0 && BigDecimal(value) != 0))
            invalid()
          if (number == 0.0) 0.0 else number
        case AttributeType.BOOLEAN if value == "true"  => true
        case AttributeType.BOOLEAN if value == "false" => false
        case _                                         => invalid()
      }
    } catch {
      case _: NumberFormatException => invalid()
    }
  }
}

private[recode] case class CompiledCategoryRecode(
    inputSchema: Schema,
    outputSchema: Schema,
    sourceIndex: Int,
    mappings: Map[Any, String],
    unmatched: UnmatchedCategoryPolicy,
    defaultCategory: String,
    missingCategory: String
) {
  def category(value: Any): String = {
    if (value == null) return missingCategory
    value match {
      case number: Double =>
        require(java.lang.Double.isFinite(number), "Nonfinite source values cannot be recoded.")
      case _ =>
    }
    mappings.getOrElse(
      value,
      unmatched match {
        case UnmatchedCategoryPolicy.SET_NULL      => null
        case UnmatchedCategoryPolicy.KEEP_ORIGINAL => value.toString
        case UnmatchedCategoryPolicy.USE_DEFAULT   => defaultCategory
        // Do not include potentially private row values in the error shown in logs/UI.
        case UnmatchedCategoryPolicy.ERROR =>
          throw new IllegalArgumentException(
            "Unmatched value in Category Recode; add a mapping or choose another policy."
          )
      }
    )
  }
}
