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

import org.apache.texera.web.resource.dashboard.VersionedResourceTables
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import javax.ws.rs.BadRequestException

class EntityTablesSpec extends AnyFlatSpec with Matchers {

  // -- BaseEntityTable --------------------------------------------------------

  "EntityTables.BaseEntityTable.apply" should "dispatch Workflow → WorkflowTable" in {
    EntityTables.BaseEntityTable(EntityType.Workflow) shouldBe
      EntityTables.BaseEntityTable.WorkflowTable
  }

  it should "dispatch Dataset → the dataset's search descriptor" in {
    // Datasets have no BaseEntityTable object: VersionedResourceTables implements it.
    EntityTables.BaseEntityTable(EntityType.Dataset) shouldBe
      VersionedResourceTables.DatasetTables
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
    EntityTables.LikeTable(EntityType.Workflow) shouldBe
      EntityTables.LikeTable.WorkflowLikeTable
  }

  it should "dispatch Dataset → DatasetLikeTable" in {
    EntityTables.LikeTable(EntityType.Dataset) shouldBe
      EntityTables.LikeTable.DatasetLikeTable
  }

  it should "dispatch Model → ModelLikeTable" in {
    EntityTables.LikeTable(EntityType.Model) shouldBe
      EntityTables.LikeTable.ModelLikeTable
  }

  "LikeTable variants" should "expose uid and the per-entity id column" in {
    val w = EntityTables.LikeTable.WorkflowLikeTable
    w.uidColumn.getName shouldBe "uid"
    w.idColumn.getName shouldBe "wid"

    val d = EntityTables.LikeTable.DatasetLikeTable
    d.uidColumn.getName shouldBe "uid"
    d.idColumn.getName shouldBe "did"

    val m = EntityTables.LikeTable.ModelLikeTable
    m.uidColumn.getName shouldBe "uid"
    m.idColumn.getName shouldBe "mid"
  }

  // -- CloneTable -------------------------------------------------------------

  "EntityTables.CloneTable.apply" should "dispatch Workflow → WorkflowCloneTable" in {
    EntityTables.CloneTable(EntityType.Workflow) shouldBe
      EntityTables.CloneTable.WorkflowCloneTable
  }

  it should "throw IllegalArgumentException for Model, which is cloneless like Dataset" in {
    val ex = intercept[IllegalArgumentException] {
      EntityTables.CloneTable(EntityType.Model)
    }
    ex.getMessage should include("Unsupported entity type")
    ex.getMessage should include("clone")
  }

  it should "throw IllegalArgumentException for Dataset because there is no DatasetClone table" in {
    // The asymmetry is intentional today: dataset clones aren't a modelled
    // entity. CloneTable.apply stays for recordCloneAction.
    val ex = intercept[IllegalArgumentException] {
      EntityTables.CloneTable(EntityType.Dataset)
    }
    ex.getMessage should include("Unsupported entity type")
    ex.getMessage should include("clone")
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
    val w = EntityTables.AccessTable(EntityType.Workflow)
    w.idColumn.getName shouldBe "wid"
    w.uidColumn.getName shouldBe "uid"
    w.privilegeColumn.getName shouldBe "privilege"

    val d = EntityTables.AccessTable(EntityType.Dataset)
    d.idColumn.getName shouldBe "did"
    d.uidColumn.getName shouldBe "uid"
    d.privilegeColumn.getName shouldBe "privilege"

    val m = EntityTables.AccessTable(EntityType.Model)
    m.idColumn.getName shouldBe "mid"
    m.uidColumn.getName shouldBe "uid"
    m.privilegeColumn.getName shouldBe "privilege"
  }

  // -- ViewCountTable ---------------------------------------------------------

  "EntityTables.ViewCountTable.apply" should "dispatch Workflow → WorkflowViewCountTable" in {
    EntityTables.ViewCountTable(EntityType.Workflow) shouldBe
      EntityTables.ViewCountTable.WorkflowViewCountTable
  }

  it should "dispatch Dataset → DatasetViewCountTable" in {
    EntityTables.ViewCountTable(EntityType.Dataset) shouldBe
      EntityTables.ViewCountTable.DatasetViewCountTable
  }

  it should "dispatch Model → ModelViewCountTable" in {
    EntityTables.ViewCountTable(EntityType.Model) shouldBe
      EntityTables.ViewCountTable.ModelViewCountTable
  }

  "ViewCountTable variants" should "expose id and view_count columns" in {
    val w = EntityTables.ViewCountTable.WorkflowViewCountTable
    w.idColumn.getName shouldBe "wid"
    w.viewCountColumn.getName shouldBe "view_count"

    val d = EntityTables.ViewCountTable.DatasetViewCountTable
    d.idColumn.getName shouldBe "did"
    d.viewCountColumn.getName shouldBe "view_count"

    val m = EntityTables.ViewCountTable.ModelViewCountTable
    m.idColumn.getName shouldBe "mid"
    m.viewCountColumn.getName shouldBe "view_count"
  }
}
