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
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.jooq.impl.DSL
import org.jooq.{Condition, Field, GroupField, Record, TableLike}

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * The one copy of FROM / WHERE / projection / hydration for every LakeFS-backed resource. A
  * concrete builder supplies only its [[VersionedResourceTables]] descriptor and the three
  * projected columns the descriptor does not already name.
  */
abstract class VersionedResourceSearchQueryBuilder[Rec <: Record, P](
    tables: VersionedResourceTables[Rec, P]
) extends SearchQueryBuilder {

  /** The resource's LakeFS repository name, which [[VersionedResourceTables.hydrate]] sizes. */
  protected val repositoryNameColumn: Field[String]

  protected val isDownloadableColumn: Field[java.lang.Boolean]

  protected val coverImageColumn: Field[String]

  /**
    * Projected for display rather than indexed, and only models carry them today. A resource
    * without such a column leaves its union slot null, which is what every other builder does.
    */
  protected val frameworkColumn: Field[String] = DSL.cast(null, classOf[String])

  protected val formatColumn: Field[String] = DSL.cast(null, classOf[String])

  /**
    * Built here rather than per-subclass, and `final` so a subclass cannot replace it, because
    * `hydrate` reads columns the subclass would otherwise have to remember to project. `userEmail`
    * is the one that bit: it defaults to `DSL.inline("")`, so omitting it cost nothing at compile
    * time and silently made `ownerEmail` null on every row. Everything else comes off the
    * descriptor, so the projection and the FROM clause cannot disagree about which columns they mean.
    *
    * `lazy` matters: the abstract members above are subclass `val`s, still null while this class's
    * constructor runs.
    */
  final override protected lazy val mappedResourceSchema: UnifiedResourceSchema =
    UnifiedResourceSchema(
      resourceType = DSL.inline(tables.resourceType),
      name = tables.nameColumn,
      description = tables.descriptionColumn,
      creationTime = tables.creationTimeColumn,
      ownerId = tables.ownerUidColumn,
      userEmail = USER.EMAIL,
      versionedResourceId = tables.idColumn,
      repositoryName = repositoryNameColumn,
      isVersionedResourcePublic = tables.isPublicColumn,
      isVersionedResourceDownloadable = isDownloadableColumn,
      versionedResourceUserAccess = tables.access.privilegeColumn,
      versionedResourceCoverImage = coverImageColumn,
      modelFramework = frameworkColumn,
      modelFormat = formatColumn
    )

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
      .and(getContainsFilter(params.owners, USER.EMAIL))
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
