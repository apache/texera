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

import org.apache.texera.amber.core.state.StateReferencing
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.filter.SpecializedFilterOpDesc
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
    "hide the loop-variable sidecar from the property panel, like operatorVersion" in {
    val schema =
      OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[SpecializedFilterOpDesc])
    val properties = schema.get("properties")
    properties.has("predicates") shouldBe true
    properties.has(StateReferencing.SIDECAR_PROPERTY) shouldBe false
    properties.has("operatorVersion") shouldBe false
    val required = schema.get("required").elements().asScala.map(_.asText()).toList
    required should not contain StateReferencing.SIDECAR_PROPERTY
    required should not contain "operatorType"
  }
}
