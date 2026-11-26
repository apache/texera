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

import org.apache.texera.web.resource.dashboard.DashboardResource.SearchQueryParams
import org.jooq.DSLContext
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

private[dashboard] object VersionedSearchQuerySupport extends Matchers {
  def params(keywords: String*): SearchQueryParams =
    SearchQueryParams(keywords = keywords.toList.asJava)

  def renderSearch(
      context: DSLContext,
      builder: SearchQueryBuilder,
      uid: Integer,
      params: SearchQueryParams,
      includePublic: Boolean
  ): String = renderSql(context.renderInlined(builder.constructQuery(uid, params, includePublic)))

  def renderSql(sql: String): String = sql.toLowerCase.replace("\"", "")

  // Compare against literals independent of the production projection. Hydration alone cannot
  // detect an alias swap that translateRecord reverses when it recreates the original columns.
  def assertProjection(sql: String, table: String, id: String): Unit = {
    val fieldAliases = Seq(
      "name" -> "resourcename",
      "description" -> "resourcedescription",
      "creation_time" -> "resourcecreationtime",
      "owner_uid" -> "resourceownerid",
      "repository_name" -> "repository_name",
      id -> "versioned_resource_id",
      "is_public" -> "is_versioned_resource_public",
      "is_downloadable" -> "is_versioned_resource_downloadable",
      "cover_image" -> "versioned_resource_cover_image"
    )
    fieldAliases.foreach {
      case (column, alias) =>
        withClue(s"$table.$column: ") {
          sql should include(s"$table.$column as $alias")
        }
    }
    sql should include(s"${table}_user_access.privilege as user_versioned_resource_access")
  }
}
