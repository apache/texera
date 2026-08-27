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

import org.apache.texera.web.resource.dashboard.FulltextSearchQueryUtils.{
  getContainsFilter,
  getDateFilter,
  getFullTextSearchFilter
}
import org.jooq.impl.DSL
import org.jooq.{Condition, GroupField, Record, TableLike}

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * The one copy of FROM / WHERE / hydration for every LakeFS-backed resource. A concrete
  * builder supplies only its [[VersionedResourceTables]] descriptor and its projection.
  */
abstract class VersionedResourceSearchQueryBuilder[Rec <: Record, P](
    tables: VersionedResourceTables[Rec, P]
) extends SearchQueryBuilder {

  /**
    * `uid` is null for anonymous callers. Visibility: public only when `uid` is null;
    * explicitly-granted only when `includePublic` is false; both when it is true.
    */
  override protected def constructFromClause(
      uid: Integer,
      params: DashboardResource.SearchQueryParams,
      includePublic: Boolean = false
  ): TableLike[_] = {
    val baseJoin = tables.joinWithAccessAndOwner(
      Some(if (uid == null) DSL.falseCondition() else tables.access.uidColumn.eq(uid))
    )

    val condition: Condition =
      if (uid == null) {
        tables.isPublicColumn.eq(true)
      } else {
        if (includePublic) {
          tables.isPublicColumn.eq(true).or(tables.access.uidColumn.isNotNull)
        } else {
          tables.access.uidColumn.isNotNull
        }
      }
    baseJoin.where(condition)
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
      tables.creationTimeColumn
    )
      .and(getContainsFilter(tables.searchIds(params), tables.idColumn))
      .and(
        getFullTextSearchFilter(splitKeywords, List(tables.nameColumn, tables.descriptionColumn))
      )
  }

  override protected def getGroupByFields: Seq[GroupField] = {
    Seq.empty
  }

  override protected def toEntryImpl(
      uid: Integer,
      record: Record
  ): DashboardResource.DashboardClickableFileEntry =
    // null = mismatch; searchAllResources drops it and flips hasMismatch.
    tables.hydrate(record, uid).map(_._2).orNull
}
