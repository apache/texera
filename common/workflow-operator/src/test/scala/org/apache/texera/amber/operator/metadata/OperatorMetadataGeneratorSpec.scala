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

package org.apache.texera.amber.operator.metadata

import com.fasterxml.jackson.databind.node.BooleanNode
import org.apache.texera.amber.core.state.StateReferencing
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.dictionary.DictionaryMatcherOpDesc
import org.apache.texera.amber.operator.filter.SpecializedFilterOpDesc
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.projection.ProjectionOpDesc
import org.apache.texera.amber.operator.sortPartitions.SortPartitionsOpDesc
import org.apache.texera.amber.operator.source.scan.file.FileScanOpDesc
import org.apache.texera.amber.operator.unneststring.UnnestStringOpDesc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.IteratorHasAsScala

class OperatorMetadataGeneratorSpec extends AnyFlatSpec with Matchers {

  "OperatorMetadataGenerator.generateOperatorMetadata" should
    "throw a RuntimeException for a class that is not a registered operator type" in {
    // the abstract base LogicalOp is not one of the concrete subtypes registered via the
    // @JsonSubTypes list on LogicalOp, so it is never collected into operatorTypeMap
    OperatorMetadataGenerator.operatorTypeMap.contains(classOf[LogicalOp]) shouldBe false

    val ex = intercept[RuntimeException] {
      OperatorMetadataGenerator.generateOperatorMetadata(classOf[LogicalOp])
    }
    ex.getMessage should include(classOf[LogicalOp].toString)
    ex.getMessage should include("is not registered")
  }

  "OperatorMetadataGenerator.generateOperatorJsonSchema" should
    "hide the loop-variable sidecar from the property panel" in {
    val schema =
      OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[SpecializedFilterOpDesc])
    val properties = schema.get("properties")
    properties.has("predicates") shouldBe true
    properties.has(StateReferencing.SIDECAR_PROPERTY) shouldBe false
    val required = schema.get("required").elements().asScala.map(_.asText()).toList
    required should not contain StateReferencing.SIDECAR_PROPERTY
  }

  /** The properties of `opDescClass`'s schema whose `noLoopVariable` keyword is `true`. */
  private def markedNoLoopVariable(opDescClass: Class[_ <: LogicalOp]): Set[String] =
    OperatorMetadataGenerator
      .generateOperatorJsonSchema(opDescClass)
      .get("properties")
      .fields()
      .asScala
      .filter(_.getValue.get("noLoopVariable") == BooleanNode.TRUE)
      .map(_.getKey)
      .toSet

  it should "mark each property that cannot hold a loop variable with 'noLoopVariable', by its JSON name" in {
    markedNoLoopVariable(classOf[ProjectionOpDesc]) shouldBe Set("isDrop", "attributes")
    // Renamed in JSON: the panel looks the property up by the name the schema gives it.
    markedNoLoopVariable(classOf[DictionaryMatcherOpDesc]) shouldBe Set("result attribute")
    markedNoLoopVariable(classOf[UnnestStringOpDesc]) shouldBe Set("Result attribute")
    // `attributeName` is declared and marked by the TextSourceOpDesc trait.
    markedNoLoopVariable(classOf[FileScanOpDesc]) shouldBe Set("attributeName", "outputFileName")
    markedNoLoopVariable(classOf[SortPartitionsOpDesc]) shouldBe
      Set("sortAttributeName", "domainMin", "domainMax")
  }

  it should "leave 'noLoopVariable' off a property the executor reads after the loop state wrote it" in {
    val properties = Seq(
      classOf[SpecializedFilterOpDesc] -> "predicates",
      classOf[LimitOpDesc] -> "limit",
      classOf[DictionaryMatcherOpDesc] -> "Dictionary",
      classOf[UnnestStringOpDesc] -> "Delimiter"
    )
    properties.foreach {
      case (opDescClass, name) =>
        val schema = OperatorMetadataGenerator.generateOperatorJsonSchema(opDescClass)
        withClue(s"${opDescClass.getSimpleName}.$name: ") {
          schema.get("properties").has(name) shouldBe true
          schema.get("properties").get(name).has("noLoopVariable") shouldBe false
        }
    }
  }

  it should "generate every operator's schema, marking exactly the properties the compiler rejects a loop variable in" in {
    // One list behind both: what the schema marks is exactly what the compiler checks.
    OperatorMetadataGenerator.operatorTypeMap.keys.foreach { opDescClass =>
      withClue(s"${opDescClass.getSimpleName}: ") {
        markedNoLoopVariable(opDescClass) shouldBe
          StateReferencing.noLoopVariableProperties(opDescClass)
      }
    }
  }
}
