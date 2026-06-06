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

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.dao.jooq.generated.Tables._
import org.scalatest.flatspec.AnyFlatSpec

class HubEntityModelSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // ActionType
  // ---------------------------------------------------------------------------

  "ActionType subtypes" should "expose their lowercase string value" in {
    assert(ActionType.View.value == "view")
    assert(ActionType.Like.value == "like")
    assert(ActionType.Clone.value == "clone")
    assert(ActionType.Unlike.value == "unlike")
  }

  it should "have toString equal to value (override pin)" in {
    // The trait override `toString = value` is what reaches log lines and
    // tracking pipelines. Pin so a regression that returns the case-object
    // name (e.g. "View" instead of "view") breaks here. Use the class
    // simpleName in the failure message — not toString, which is what
    // we are pinning — so a regression produces a readable diagnostic.
    val all: List[ActionType] =
      List(ActionType.View, ActionType.Like, ActionType.Clone, ActionType.Unlike)
    all.foreach { a =>
      val name = a.getClass.getSimpleName
      assert(a.toString == a.value, s"$name.toString = '${a.toString}' but value = '${a.value}'")
    }
  }

  "ActionType.fromString" should "match each subtype exactly" in {
    assert(ActionType.fromString("view") == ActionType.View)
    assert(ActionType.fromString("like") == ActionType.Like)
    assert(ActionType.fromString("clone") == ActionType.Clone)
    assert(ActionType.fromString("unlike") == ActionType.Unlike)
  }

  it should "match case-insensitively" in {
    assert(ActionType.fromString("VIEW") == ActionType.View)
    assert(ActionType.fromString("Like") == ActionType.Like)
    assert(ActionType.fromString("CLONE") == ActionType.Clone)
    assert(ActionType.fromString("uNlIkE") == ActionType.Unlike)
  }

  it should "throw IllegalArgumentException for an unknown action, naming the input" in {
    val ex = intercept[IllegalArgumentException] {
      ActionType.fromString("delete")
    }
    assert(ex.getMessage.contains("delete"), s"unexpected message: ${ex.getMessage}")
  }

  it should "throw IllegalArgumentException for an empty string, naming the empty input" in {
    // Pin the concrete `''` representation in the message, not just that
    // an exception is thrown (`"".contains("")` is trivially true).
    val ex = intercept[IllegalArgumentException] {
      ActionType.fromString("")
    }
    assert(ex.getMessage.contains("''"), s"unexpected message: ${ex.getMessage}")
  }

  "ActionType Jackson round-trip" should "serialize each subtype as its lowercase string value" in {
    // `@JsonValue` on `value` instructs Jackson to emit the field's value
    // string instead of a wrapping object form.
    assert(objectMapper.writeValueAsString(ActionType.View: ActionType) == "\"view\"")
    assert(objectMapper.writeValueAsString(ActionType.Like: ActionType) == "\"like\"")
    assert(objectMapper.writeValueAsString(ActionType.Clone: ActionType) == "\"clone\"")
    assert(objectMapper.writeValueAsString(ActionType.Unlike: ActionType) == "\"unlike\"")
  }

  it should "deserialize from the lowercase string back to the canonical subtype" in {
    // `@JsonCreator` on `fromString` lets Jackson reconstruct the subtype
    // from a raw string field.
    assert(objectMapper.readValue("\"view\"", classOf[ActionType]) == ActionType.View)
    assert(objectMapper.readValue("\"like\"", classOf[ActionType]) == ActionType.Like)
    assert(objectMapper.readValue("\"clone\"", classOf[ActionType]) == ActionType.Clone)
    assert(objectMapper.readValue("\"unlike\"", classOf[ActionType]) == ActionType.Unlike)
  }

  it should "honor the case-insensitive deserialization via @JsonCreator" in {
    assert(objectMapper.readValue("\"VIEW\"", classOf[ActionType]) == ActionType.View)
  }

  // ---------------------------------------------------------------------------
  // EntityType
  // ---------------------------------------------------------------------------

  "EntityType subtypes" should "expose their lowercase string value" in {
    assert(EntityType.Workflow.value == "workflow")
    assert(EntityType.Dataset.value == "dataset")
  }

  it should "have toString equal to value (override pin)" in {
    // Same stable-name pattern as ActionType — don't use the SUT
    // (toString) in the failure message.
    val all: List[EntityType] = List(EntityType.Workflow, EntityType.Dataset)
    all.foreach { e =>
      val name = e.getClass.getSimpleName
      assert(e.toString == e.value, s"$name.toString = '${e.toString}' but value = '${e.value}'")
    }
  }

  "EntityType.fromString" should "match each subtype exactly" in {
    assert(EntityType.fromString("workflow") == EntityType.Workflow)
    assert(EntityType.fromString("dataset") == EntityType.Dataset)
  }

  it should "match case-insensitively" in {
    assert(EntityType.fromString("WORKFLOW") == EntityType.Workflow)
    assert(EntityType.fromString("Dataset") == EntityType.Dataset)
  }

  it should "throw IllegalArgumentException for an unknown kind, naming the input" in {
    val ex = intercept[IllegalArgumentException] {
      EntityType.fromString("project")
    }
    assert(ex.getMessage.contains("project"))
  }

  it should "throw IllegalArgumentException for an empty string, naming the empty input" in {
    val ex = intercept[IllegalArgumentException] {
      EntityType.fromString("")
    }
    assert(ex.getMessage.contains("''"), s"unexpected message: ${ex.getMessage}")
  }

  "EntityType Jackson round-trip" should "serialize / deserialize each subtype as its lowercase string value" in {
    assert(objectMapper.writeValueAsString(EntityType.Workflow: EntityType) == "\"workflow\"")
    assert(objectMapper.writeValueAsString(EntityType.Dataset: EntityType) == "\"dataset\"")
    assert(objectMapper.readValue("\"workflow\"", classOf[EntityType]) == EntityType.Workflow)
    assert(objectMapper.readValue("\"dataset\"", classOf[EntityType]) == EntityType.Dataset)
  }

  // ---------------------------------------------------------------------------
  // EntityTables.BaseEntityTable
  // ---------------------------------------------------------------------------

  "EntityTables.BaseEntityTable.apply" should "dispatch Workflow → WorkflowTable" in {
    val t = EntityTables.BaseEntityTable(EntityType.Workflow)
    assert(t == EntityTables.BaseEntityTable.WorkflowTable)
    assert(t.table == WORKFLOW)
    assert(t.isPublicColumn == WORKFLOW.IS_PUBLIC)
    assert(t.idColumn == WORKFLOW.WID)
  }

  it should "dispatch Dataset → DatasetTable" in {
    val t = EntityTables.BaseEntityTable(EntityType.Dataset)
    assert(t == EntityTables.BaseEntityTable.DatasetTable)
    assert(t.table == DATASET)
    assert(t.isPublicColumn == DATASET.IS_PUBLIC)
    assert(t.idColumn == DATASET.DID)
  }

  // ---------------------------------------------------------------------------
  // EntityTables.LikeTable
  // ---------------------------------------------------------------------------

  "EntityTables.LikeTable.apply" should "dispatch Workflow → WorkflowLikeTable" in {
    val t = EntityTables.LikeTable(EntityType.Workflow)
    assert(t == EntityTables.LikeTable.WorkflowLikeTable)
    assert(t.table == WORKFLOW_USER_LIKES)
    assert(t.uidColumn == WORKFLOW_USER_LIKES.UID)
    assert(t.idColumn == WORKFLOW_USER_LIKES.WID)
  }

  it should "dispatch Dataset → DatasetLikeTable" in {
    val t = EntityTables.LikeTable(EntityType.Dataset)
    assert(t == EntityTables.LikeTable.DatasetLikeTable)
    assert(t.table == DATASET_USER_LIKES)
    assert(t.uidColumn == DATASET_USER_LIKES.UID)
    assert(t.idColumn == DATASET_USER_LIKES.DID)
  }

  // ---------------------------------------------------------------------------
  // EntityTables.CloneTable (workflow-only today)
  // ---------------------------------------------------------------------------

  "EntityTables.CloneTable.apply" should "dispatch Workflow → WorkflowCloneTable" in {
    val t = EntityTables.CloneTable(EntityType.Workflow)
    assert(t == EntityTables.CloneTable.WorkflowCloneTable)
    assert(t.table == WORKFLOW_USER_CLONES)
    assert(t.uidColumn == WORKFLOW_USER_CLONES.UID)
    assert(t.idColumn == WORKFLOW_USER_CLONES.WID)
  }

  it should "throw IllegalArgumentException for Dataset (clone is workflow-only)" in {
    // Pin: clone is implemented for workflow only today. A future addition
    // of DatasetCloneTable should remove this assertion deliberately.
    val ex = intercept[IllegalArgumentException] {
      EntityTables.CloneTable(EntityType.Dataset)
    }
    assert(ex.getMessage.contains("clone"))
  }

  // ---------------------------------------------------------------------------
  // EntityTables.ViewCountTable
  // ---------------------------------------------------------------------------

  "EntityTables.ViewCountTable.apply" should "dispatch Workflow → WorkflowViewCountTable" in {
    val t = EntityTables.ViewCountTable(EntityType.Workflow)
    assert(t == EntityTables.ViewCountTable.WorkflowViewCountTable)
    assert(t.table == WORKFLOW_VIEW_COUNT)
    assert(t.idColumn == WORKFLOW_VIEW_COUNT.WID)
    assert(t.viewCountColumn == WORKFLOW_VIEW_COUNT.VIEW_COUNT)
  }

  it should "dispatch Dataset → DatasetViewCountTable" in {
    val t = EntityTables.ViewCountTable(EntityType.Dataset)
    assert(t == EntityTables.ViewCountTable.DatasetViewCountTable)
    assert(t.table == DATASET_VIEW_COUNT)
    assert(t.idColumn == DATASET_VIEW_COUNT.DID)
    assert(t.viewCountColumn == DATASET_VIEW_COUNT.VIEW_COUNT)
  }
}
