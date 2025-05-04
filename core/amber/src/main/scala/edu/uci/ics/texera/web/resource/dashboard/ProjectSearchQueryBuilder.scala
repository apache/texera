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

package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.dao.jooq.generated.Tables.{PROJECT, PROJECT_USER_ACCESS}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.Project
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.DashboardClickableFileEntry
import edu.uci.ics.texera.web.resource.dashboard.FulltextSearchQueryUtils.{
  getContainsFilter,
  getDateFilter,
  getFullTextSearchFilter
}
import org.jooq.impl.DSL

import org.jooq.{Condition, GroupField, Record, TableLike}

import scala.jdk.CollectionConverters.CollectionHasAsScala

object ProjectSearchQueryBuilder extends SearchQueryBuilder {

  override val mappedResourceSchema: UnifiedResourceSchema = UnifiedResourceSchema(
    resourceType = DSL.inline(SearchQueryBuilder.PROJECT_RESOURCE_TYPE),
    name = PROJECT.NAME,
    description = PROJECT.DESCRIPTION,
    creationTime = PROJECT.CREATION_TIME,
    lastModifiedTime = PROJECT.CREATION_TIME,
    pid = PROJECT.PID,
    ownerId = PROJECT.OWNER_ID,
    projectColor = PROJECT.COLOR
  )

  override protected def constructFromClause(
      uid: Integer,
      params: DashboardResource.SearchQueryParams,
      includePublic: Boolean = false
  ): TableLike[_] = {
    PROJECT
      .leftJoin(PROJECT_USER_ACCESS)
      .on(PROJECT_USER_ACCESS.PID.eq(PROJECT.PID))
      .where(PROJECT_USER_ACCESS.UID.eq(uid))
  }

  override protected def constructWhereClause(
      uid: Integer,
      params: DashboardResource.SearchQueryParams
  ): Condition = {
    val splitKeywords = params.keywords.asScala
      .flatMap(_.split("[+\\-()<>~*@\"]"))
      .filter(_.nonEmpty)
      .toSeq

    getDateFilter(
      params.creationStartDate,
      params.creationEndDate,
      PROJECT.CREATION_TIME
    )
      .and(getContainsFilter(params.projectIds, PROJECT.PID))
      .and(
        getFullTextSearchFilter(splitKeywords, List(PROJECT.NAME, PROJECT.DESCRIPTION))
      )
  }

  override protected def getGroupByFields: Seq[GroupField] = Seq.empty

  override def toEntryImpl(
      uid: Integer,
      record: Record
  ): DashboardResource.DashboardClickableFileEntry = {
    val dp = record.into(PROJECT).into(classOf[Project])
    DashboardClickableFileEntry(SearchQueryBuilder.PROJECT_RESOURCE_TYPE, project = Some(dp))
  }
}
