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

import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.web.resource.dashboard.VersionedResourceTables
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import javax.ws.rs.BadRequestException

class EntityTablesSpec extends AnyFlatSpec with Matchers {

  // -- BaseEntityTable --------------------------------------------------------

  "EntityTables.BaseEntityTable.apply" should "dispatch Workflow → WorkflowTable" in {
    val descriptor = EntityTables.BaseEntityTable(EntityType.Workflow)
    descriptor shouldBe EntityTables.BaseEntityTable.WorkflowTable
    descriptor.table shouldBe WORKFLOW
    descriptor.isPublicColumn shouldBe WORKFLOW.IS_PUBLIC
    descriptor.idColumn shouldBe WORKFLOW.WID
  }

  it should "dispatch Dataset → the dataset's search descriptor" in {
    // Datasets have no BaseEntityTable object: VersionedResourceTables implements it.
    val descriptor = EntityTables.BaseEntityTable(EntityType.Dataset)
    descriptor shouldBe VersionedResourceTables.DatasetTables
    descriptor.table shouldBe DATASET
    descriptor.isPublicColumn shouldBe DATASET.IS_PUBLIC
    descriptor.idColumn shouldBe DATASET.DID
  }

  it should "dispatch Model → the model's search descriptor" in {
    EntityTables.BaseEntityTable(EntityType.Model) shouldBe
      VersionedResourceTables.ModelTables
  }

  "BaseEntityTable.WorkflowTable" should "wire up id and isPublic columns from WORKFLOW" in {
    val t = EntityTables.BaseEntityTable.WorkflowTable
    t.idColumn.getName shouldBe "wid"
    t.isPublicColumn.getName shouldBe "is_public"
  }

  "VersionedResourceTables.DatasetTables" should "wire up id and isPublic columns from DATASET" in {
    val t = VersionedResourceTables.DatasetTables
    t.idColumn.getName shouldBe "did"
    t.isPublicColumn.getName shouldBe "is_public"
  }

  "VersionedResourceTables.ModelTables" should "wire up id and isPublic columns from MODEL" in {
    val t = VersionedResourceTables.ModelTables
    t.idColumn.getName shouldBe "mid"
    t.isPublicColumn.getName shouldBe "is_public"
  }

  // -- LikeTable --------------------------------------------------------------

  "EntityTables.LikeTable.apply" should "dispatch Workflow → WorkflowLikeTable" in {
    val descriptor = EntityTables.LikeTable(EntityType.Workflow)
    descriptor shouldBe EntityTables.LikeTable.WorkflowLikeTable
    descriptor.table shouldBe WORKFLOW_USER_LIKES
    descriptor.uidColumn shouldBe WORKFLOW_USER_LIKES.UID
    descriptor.idColumn shouldBe WORKFLOW_USER_LIKES.WID
  }

  it should "dispatch Dataset → DatasetLikeTable" in {
    val descriptor = EntityTables.LikeTable(EntityType.Dataset)
    descriptor shouldBe EntityTables.LikeTable.DatasetLikeTable
    descriptor.table shouldBe DATASET_USER_LIKES
    descriptor.uidColumn shouldBe DATASET_USER_LIKES.UID
    descriptor.idColumn shouldBe DATASET_USER_LIKES.DID
  }

  it should "dispatch Model → ModelLikeTable" in {
    EntityTables.LikeTable(EntityType.Model) shouldBe
      EntityTables.LikeTable.ModelLikeTable
  }

  "LikeTable variants" should "expose uid and the per-entity id column" in {
    import EntityTables.LikeTable._
    Seq(WorkflowLikeTable -> "wid", DatasetLikeTable -> "did", ModelLikeTable -> "mid").foreach {
      case (table, id) =>
        table.uidColumn.getName shouldBe "uid"
        table.idColumn.getName shouldBe id
    }
  }

  // -- CloneTable -------------------------------------------------------------

  "EntityTables.CloneTable.apply" should "dispatch Workflow → WorkflowCloneTable" in {
    val descriptor = EntityTables.CloneTable(EntityType.Workflow)
    descriptor shouldBe EntityTables.CloneTable.WorkflowCloneTable
    descriptor.table shouldBe WORKFLOW_USER_CLONES
    descriptor.uidColumn shouldBe WORKFLOW_USER_CLONES.UID
    descriptor.idColumn shouldBe WORKFLOW_USER_CLONES.WID
  }

  private def assertCloneless(entityType: EntityType): Unit = {
    val ex = intercept[IllegalArgumentException](EntityTables.CloneTable(entityType))
    ex.getMessage should include("Unsupported entity type")
    ex.getMessage should include("clone")
  }

  it should "throw IllegalArgumentException for Model, which is cloneless like Dataset" in {
    assertCloneless(EntityType.Model)
  }

  it should "throw IllegalArgumentException for Dataset because there is no DatasetClone table" in {
    assertCloneless(EntityType.Dataset)
  }

  // -- the registry -----------------------------------------------------------

  "EntityTables.apply" should "expose every table an entity type owns" in {
    val workflow = EntityTables(EntityType.Workflow)
    workflow.base shouldBe EntityTables.BaseEntityTable.WorkflowTable
    workflow.like shouldBe EntityTables.LikeTable.WorkflowLikeTable
    workflow.viewCount shouldBe EntityTables.ViewCountTable.WorkflowViewCountTable
    workflow.access shouldBe EntityTables.AccessTable.WorkflowAccessTable
    workflow.cloneTable shouldBe Some(EntityTables.CloneTable.WorkflowCloneTable)
    workflow.versionedResource shouldBe None

    val dataset = EntityTables(EntityType.Dataset)
    dataset.base shouldBe VersionedResourceTables.DatasetTables
    dataset.like shouldBe EntityTables.LikeTable.DatasetLikeTable
    dataset.viewCount shouldBe EntityTables.ViewCountTable.DatasetViewCountTable
    dataset.access shouldBe EntityTables.AccessTable.DatasetAccessTable
    dataset.cloneTable shouldBe None
    dataset.versionedResource shouldBe Some(VersionedResourceTables.DatasetTables)

    val model = EntityTables(EntityType.Model)
    model.base shouldBe VersionedResourceTables.ModelTables
    model.like shouldBe EntityTables.LikeTable.ModelLikeTable
    model.viewCount shouldBe EntityTables.ViewCountTable.ModelViewCountTable
    model.access shouldBe EntityTables.AccessTable.ModelAccessTable
    model.cloneTable shouldBe None
    model.versionedResource shouldBe Some(VersionedResourceTables.ModelTables)
  }

  it should "reject a missing entity type" in {
    val error = intercept[BadRequestException](EntityTables(null))
    error.getMessage shouldBe "Missing entityType"
  }

  // -- AccessTable ------------------------------------------------------------

  "EntityTables.AccessTable" should "expose id, uid and privilege per entity" in {
    Seq(EntityType.Workflow -> "wid", EntityType.Dataset -> "did", EntityType.Model -> "mid")
      .foreach {
        case (entityType, id) =>
          val table = EntityTables.AccessTable(entityType)
          table.idColumn.getName shouldBe id
          table.uidColumn.getName shouldBe "uid"
          table.privilegeColumn.getName shouldBe "privilege"
      }
  }

  // -- ViewCountTable ---------------------------------------------------------

  "EntityTables.ViewCountTable.apply" should "dispatch Workflow → WorkflowViewCountTable" in {
    val descriptor = EntityTables.ViewCountTable(EntityType.Workflow)
    descriptor shouldBe EntityTables.ViewCountTable.WorkflowViewCountTable
    descriptor.table shouldBe WORKFLOW_VIEW_COUNT
    descriptor.idColumn shouldBe WORKFLOW_VIEW_COUNT.WID
    descriptor.viewCountColumn shouldBe WORKFLOW_VIEW_COUNT.VIEW_COUNT
  }

  it should "dispatch Dataset → DatasetViewCountTable" in {
    val descriptor = EntityTables.ViewCountTable(EntityType.Dataset)
    descriptor shouldBe EntityTables.ViewCountTable.DatasetViewCountTable
    descriptor.table shouldBe DATASET_VIEW_COUNT
    descriptor.idColumn shouldBe DATASET_VIEW_COUNT.DID
    descriptor.viewCountColumn shouldBe DATASET_VIEW_COUNT.VIEW_COUNT
  }

  it should "dispatch Model → ModelViewCountTable" in {
    EntityTables.ViewCountTable(EntityType.Model) shouldBe
      EntityTables.ViewCountTable.ModelViewCountTable
  }

  "ViewCountTable variants" should "expose id and view_count columns" in {
    import EntityTables.ViewCountTable._
    Seq(
      WorkflowViewCountTable -> "wid",
      DatasetViewCountTable -> "did",
      ModelViewCountTable -> "mid"
    ).foreach {
      case (table, id) =>
        table.idColumn.getName shouldBe id
        table.viewCountColumn.getName shouldBe "view_count"
    }
  }
}
