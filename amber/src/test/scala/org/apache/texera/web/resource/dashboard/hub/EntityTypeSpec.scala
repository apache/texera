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

package org.apache.texera.web.resource.dashboard.hub

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EntityTypeSpec extends AnyFlatSpec with Matchers {

  "EntityType.value" should "be the lowercase form of the case-object name" in {
    EntityType.Workflow.value shouldBe "workflow"
    EntityType.Dataset.value shouldBe "dataset"
  }

  "EntityType.toString" should "mirror value (used by error messages and SQL strings)" in {
    EntityType.Workflow.toString shouldBe "workflow"
    EntityType.Dataset.toString shouldBe "dataset"
  }

  "EntityType.fromString" should "round-trip the canonical lowercase value" in {
    EntityType.fromString("workflow") shouldBe EntityType.Workflow
    EntityType.fromString("dataset") shouldBe EntityType.Dataset
  }

  it should "accept mixed case (equalsIgnoreCase)" in {
    EntityType.fromString("Workflow") shouldBe EntityType.Workflow
    EntityType.fromString("DATASET") shouldBe EntityType.Dataset
  }

  it should "throw IllegalArgumentException for an unknown value" in {
    val ex = intercept[IllegalArgumentException] {
      EntityType.fromString("project")
    }
    ex.getMessage should include("project")
  }

  // The @JsonValue / @JsonCreator pair drives Jackson serialisation. Cover
  // the round-trip so a future @JsonValue rename can't silently change the
  // wire format.
  "Jackson serialisation" should "render an EntityType as its lowercase value and parse it back" in {
    val mapper = new ObjectMapper()
    mapper.writeValueAsString(EntityType.Workflow: EntityType) shouldBe "\"workflow\""
    mapper.readValue("\"dataset\"", classOf[EntityType]) shouldBe EntityType.Dataset
  }
}
