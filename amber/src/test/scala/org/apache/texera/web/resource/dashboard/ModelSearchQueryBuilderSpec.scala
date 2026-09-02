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
import org.apache.texera.web.resource.dashboard.DashboardResource.SearchQueryParams
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/**
  * `ModelSearchQueryBuilder` is a projection over the shared query logic, so this suite pins what
  * is model-specific: the columns each schema slot carries, the tables the access scoping reads,
  * and the id list the filter comes from. The FROM/WHERE/hydration behaviour itself is covered
  * once, in `DatasetSearchQueryBuilderSpec` and `VersionedResourceTablesSpec`.
  *
  * SQL is rendered, never executed, so no lakeFS stub is needed here. Keyword assertions stay on
  * the tokens and the `coalesce(...) || ' ' || coalesce(...)` expression, which
  * `FulltextSearchQueryUtils` builds before its pgroonga/tsvector branch — an assertion on either
  * arm would depend on which suites ran earlier in the JVM.
  */
class ModelSearchQueryBuilderSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  private val callerUid: Integer = Integer.valueOf(9201)

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  private def params(keywords: String*): SearchQueryParams =
    SearchQueryParams(keywords = keywords.toList.asJava)

  private def sqlFor(
      uid: Integer,
      includePublic: Boolean,
      p: SearchQueryParams = params()
  ): String =
    getDSLContext
      .renderInlined(ModelSearchQueryBuilder.constructQuery(uid, p, includePublic))
      .toLowerCase
      .replace("\"", "")

  "the projection" should "carry every model column under the alias its schema slot names" in {
    val sql = sqlFor(callerUid, includePublic = true)

    sql should include("'model' as resourcetype")
    sql should include("model.name as resourcename")
    sql should include("model.description as resourcedescription")
    sql should include("model.creation_time as resourcecreationtime")
    sql should include("model.owner_uid as resourceownerid")
    sql should include("model.repository_name as repository_name")
    // Shared with the other versioned resources, hence the resource-neutral aliases.
    sql should include("model.mid as versioned_resource_id")
    sql should include("model.is_public as is_versioned_resource_public")
    sql should include("model.is_downloadable as is_versioned_resource_downloadable")
    sql should include("model_user_access.privilege as user_versioned_resource_access")
    sql should include("model.cover_image as versioned_resource_cover_image")
  }

  it should "carry framework and format, which no other resource projects" in {
    val sql = sqlFor(callerUid, includePublic = true)

    sql should include("model.framework as model_framework")
    sql should include("model.format as model_format")
  }

  it should "leave the workflow, project and dataset-only slots null" in {
    // The union requires all builders to project the same 26 columns in the same order.
    val sql = sqlFor(callerUid, includePublic = true)

    sql should include("cast(null as int) as wid")
    sql should include("cast(null as int) as pid")
    sql should include("cast(null as varchar) as dataset_storage_path")
    sql should include("cast(null as varchar) as workflow_cover_image")
  }

  "access scoping" should "read the model's own tables" in {
    val sql = sqlFor(callerUid, includePublic = true)

    sql should include("from texera_db.model")
    sql should include regex "model_user_access\\.mid = [\\w.]*model\\.mid"
    sql should include("model_user_access.uid = 9201")
  }

  it should "restrict an anonymous caller to public models" in {
    val sql = sqlFor(null, includePublic = false)

    sql should include("model.is_public = true")
    sql should not include "model_user_access.uid is not null"
  }

  it should "return only granted models when public ones are excluded" in {
    val sql = sqlFor(callerUid, includePublic = false)

    sql should include("model_user_access.uid is not null")
    sql should not include "model.is_public = true"
  }

  it should "add public models to the granted ones when they are included" in {
    val sql = sqlFor(callerUid, includePublic = true)

    sql should include("model.is_public = true")
    sql should include("model_user_access.uid is not null")
  }

  "the id filter" should "come from modelIds, and ignore datasetIds" in {
    val sql = sqlFor(
      callerUid,
      includePublic = true,
      SearchQueryParams(
        modelIds = List(Integer.valueOf(77)).asJava,
        datasetIds = List(Integer.valueOf(88)).asJava
      )
    )

    // jOOQ collapses a single-element IN to an equality.
    sql should include("model.mid = 77")
    sql should not include "88"
  }

  "the keyword filter" should "search the model's name and its description" in {
    val sql = sqlFor(callerUid, includePublic = true, params("resnet"))

    sql should include("resnet")
    sql should include regex "coalesce\\([\\w.]*model\\.name.*coalesce\\([\\w.]*model\\.description"
  }

  it should "not filter at all when no keyword is given" in {
    sqlFor(callerUid, includePublic = true) should not include "coalesce"
  }

  "the query" should "dedupe with selectDistinct rather than a group by" in {
    val sql = sqlFor(callerUid, includePublic = true)

    sql should include("select distinct")
    sql should not include "group by"
  }
}
