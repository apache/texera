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

package org.apache.texera.web.resource.dashboard

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, Model}
import org.apache.texera.web.resource.dashboard.DashboardResource.SearchQueryParams
import org.apache.texera.web.resource.dashboard.hub.EntityTables
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/**
  * The descriptor is the only place a resource's columns are named, so a slot wired to the
  * wrong column still compiles and runs — it just queries the wrong field. SQL is rendered,
  * never executed. `repositorySize` is uncovered: live LakeFS call, no mockable seam.
  */
class VersionedResourceTablesSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  private val tables = VersionedResourceTables.DatasetTables
  private val modelTables = VersionedResourceTables.ModelTables

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  private def rendered(sql: String): String = sql.toLowerCase.replace("\"", "")

  private def model(mid: Int, ownerUid: Int, repositoryName: String): Model = {
    val m = new Model
    m.setMid(Integer.valueOf(mid))
    m.setOwnerUid(Integer.valueOf(ownerUid))
    m.setRepositoryName(repositoryName)
    m
  }

  private def dataset(did: Int, ownerUid: Int, repositoryName: String): Dataset = {
    val d = new Dataset
    d.setDid(Integer.valueOf(did))
    d.setOwnerUid(Integer.valueOf(ownerUid))
    d.setRepositoryName(repositoryName)
    d
  }

  "DatasetTables" should "name every column the shared query logic reads" in {
    tables.table.getName shouldBe "dataset"
    tables.idColumn.getName shouldBe "did"
    tables.isPublicColumn.getName shouldBe "is_public"
    tables.nameColumn.getName shouldBe "name"
    tables.descriptionColumn.getName shouldBe "description"
    tables.creationTimeColumn.getName shouldBe "creation_time"
    tables.ownerUidColumn.getName shouldBe "owner_uid"
  }

  it should "share the hub's access table rather than naming it twice" in {
    tables.access shouldBe EntityTables.AccessTable.DatasetAccessTable
  }

  it should "tag its rows with the resource type DashboardResource dispatches on" in {
    // DashboardResource matches this literal with no default branch.
    tables.resourceType shouldBe SearchQueryBuilder.DATASET_RESOURCE_TYPE
    tables.resourceType shouldBe "dataset"
  }

  it should "read its id filter out of the shared search params" in {
    // A sibling's param list here would silently apply the wrong filter.
    val ids = List(Integer.valueOf(7), Integer.valueOf(9)).asJava
    tables.searchIds(SearchQueryParams(datasetIds = ids)) shouldBe ids
    tables.searchIds(SearchQueryParams()) shouldBe empty
  }

  it should "expose the POJO fields the hub de-dupes and sizes by" in {
    val d = dataset(11, 22, "dataset-11")
    tables.idOf(d) shouldBe Integer.valueOf(11)
    tables.ownerUidOf(d) shouldBe Integer.valueOf(22)
    tables.repositoryNameOf(d) shouldBe "dataset-11"
  }

  "entry" should "wrap the resource in the slot the frontend reads it from" in {
    val d = dataset(11, 22, "dataset-11")
    val entry = tables.entry(d, "owner@test.com", PrivilegeEnum.READ, isOwner = true, size = 512L)

    entry.resourceType shouldBe "dataset"
    entry.workflow shouldBe None
    val dashboardDataset = entry.dataset.getOrElse(fail("expected a dataset entry"))
    dashboardDataset.dataset shouldBe d
    dashboardDataset.ownerEmail shouldBe "owner@test.com"
    dashboardDataset.accessPrivilege shouldBe PrivilegeEnum.READ
    dashboardDataset.isOwner shouldBe true
    dashboardDataset.size shouldBe 512L
  }

  "joinWithAccessAndOwner" should "join the access rows and the owner" in {
    // Rendered names are schema-qualified (texera_db.dataset.did), hence the regex.
    val sql = rendered(getDSLContext.render(tables.joinWithAccessAndOwner(None)))

    sql should include regex "dataset_user_access\\.did = [\\w.]*dataset\\.did"
    sql should include regex "user\\.uid = [\\w.]*dataset\\.owner_uid"
    // Both joins are outer: a resource with no grant row must still appear.
    sql.split("left outer join").length shouldBe 3
  }

  it should "leave the access rows unfiltered when no condition is given" in {
    // The hub's call: every grant row.
    val sql = rendered(getDSLContext.render(tables.joinWithAccessAndOwner(None)))

    sql should not include "dataset_user_access.uid ="
  }

  it should "narrow the access rows when a condition is given" in {
    // The search's call. Without the predicate, `uid is not null` matches anybody's grant.
    val sql = rendered(
      getDSLContext.renderInlined(
        tables.joinWithAccessAndOwner(Some(tables.access.uidColumn.eq(Integer.valueOf(42))))
      )
    )

    sql should include("dataset_user_access.uid = 42")
  }

  "ModelTables" should "name every column the shared query logic reads" in {
    modelTables.table.getName shouldBe "model"
    modelTables.idColumn.getName shouldBe "mid"
    modelTables.isPublicColumn.getName shouldBe "is_public"
    modelTables.nameColumn.getName shouldBe "name"
    modelTables.descriptionColumn.getName shouldBe "description"
    modelTables.creationTimeColumn.getName shouldBe "creation_time"
    modelTables.ownerUidColumn.getName shouldBe "owner_uid"
  }

  it should "share the hub's access table rather than naming it twice" in {
    modelTables.access shouldBe EntityTables.AccessTable.ModelAccessTable
  }

  it should "tag its rows with the resource type DashboardResource dispatches on" in {
    modelTables.resourceType shouldBe SearchQueryBuilder.MODEL_RESOURCE_TYPE
    modelTables.resourceType shouldBe "model"
  }

  it should "read its id filter out of modelIds, not the dataset's list" in {
    val modelIds = List(Integer.valueOf(3)).asJava
    val datasetIds = List(Integer.valueOf(4)).asJava
    val params = SearchQueryParams(datasetIds = datasetIds, modelIds = modelIds)

    modelTables.searchIds(params) shouldBe modelIds
    tables.searchIds(params) shouldBe datasetIds
  }

  it should "expose the POJO fields the hub de-dupes and sizes by" in {
    val m = model(31, 42, "model-31")
    modelTables.idOf(m) shouldBe Integer.valueOf(31)
    modelTables.ownerUidOf(m) shouldBe Integer.valueOf(42)
    modelTables.repositoryNameOf(m) shouldBe "model-31"
  }

  it should "wrap the model in the model slot, leaving the dataset slot empty" in {
    val m = model(31, 42, "model-31")
    val entry =
      modelTables.entry(m, "owner@test.com", PrivilegeEnum.WRITE, isOwner = false, size = 2048L)

    entry.resourceType shouldBe "model"
    entry.workflow shouldBe None
    entry.dataset shouldBe None
    val dashboardModel = entry.model.getOrElse(fail("expected a model entry"))
    dashboardModel.model shouldBe m
    dashboardModel.ownerEmail shouldBe "owner@test.com"
    dashboardModel.accessPrivilege shouldBe PrivilegeEnum.WRITE
    dashboardModel.isOwner shouldBe false
    dashboardModel.size shouldBe 2048L
  }

  it should "join the model's own access and owner rows" in {
    val sql = rendered(getDSLContext.render(modelTables.joinWithAccessAndOwner(None)))

    sql should include regex "model_user_access\\.mid = [\\w.]*model\\.mid"
    sql should include regex "user\\.uid = [\\w.]*model\\.owner_uid"
    sql.split("left outer join").length shouldBe 3
  }

}
