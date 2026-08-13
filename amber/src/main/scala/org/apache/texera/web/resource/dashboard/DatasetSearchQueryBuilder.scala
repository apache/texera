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

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.dao.jooq.generated.Tables.{DATASET, DATASET_USER_ACCESS}
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, User}
import org.apache.texera.web.resource.dashboard.DashboardResource.DashboardClickableFileEntry
import org.apache.texera.web.resource.dashboard.FulltextSearchQueryUtils.{
  getContainsFilter,
  getDateFilter,
  getFullTextSearchFilter
}
import org.apache.texera.web.resource.dashboard.user.dataset.DatasetResource.DashboardDataset
import org.jooq.impl.DSL
import org.jooq.{Condition, GroupField, Record, TableLike}

import scala.jdk.CollectionConverters.CollectionHasAsScala

object DatasetSearchQueryBuilder extends SearchQueryBuilder with LazyLogging {
  override protected val mappedResourceSchema: UnifiedResourceSchema = UnifiedResourceSchema(
    resourceType = DSL.inline(SearchQueryBuilder.DATASET_RESOURCE_TYPE),
    name = DATASET.NAME,
    description = DATASET.DESCRIPTION,
    creationTime = DATASET.CREATION_TIME,
    ownerId = DATASET.OWNER_UID,
    did = DATASET.DID,
    repositoryName = DATASET.REPOSITORY_NAME,
    isDatasetPublic = DATASET.IS_PUBLIC,
    isDatasetDownloadable = DATASET.IS_DOWNLOADABLE,
    datasetUserAccess = DATASET_USER_ACCESS.PRIVILEGE,
    datasetCoverImage = DATASET.COVER_IMAGE
  )

  /*
   * constructs the FROM clause for querying datasets with specific access controls.
   *
   * Parameter:
   * - uid: Integer - Represents the unique identifier of the current user.
   *  - uid is 'null' if the user is not logged in or performing a public search.
   *  - Otherwise, `uid` holds the identifier for the logged-in user.
   * - includePublic - Boolean - Specifies whether to include public datasets in the result.
   */
  override protected def constructFromClause(
      uid: Integer,
      params: DashboardResource.SearchQueryParams,
      includePublic: Boolean = false
  ): TableLike[_] = {
    // Case 1: if `uid` is (set) and `includePublic` is false
    // -> return ONLY datasets that given `uid` has explicit access to.
    // Case 2: if `uid` is (null) and `includePublic` is true
    // -> return ONLY datasets that are public
    // Case 3: if `uid` is (set) and `includePublic` is true
    // -> Union of datasets that are public and explicitly shared with user is returned
    // Case 4: if `uid` is (null) and `includePublic` is false
    // -> return public datasets by default as user might not be logged in
    val baseJoin = DATASET
      .leftJoin(DATASET_USER_ACCESS)
      .on(DATASET_USER_ACCESS.DID.eq(DATASET.DID))
      .and(if (uid == null) DSL.falseCondition() else DATASET_USER_ACCESS.UID.eq(uid))
      .leftJoin(USER)
      .on(USER.UID.eq(DATASET.OWNER_UID))

    // Set the `condition` where clause here
    val condition: Condition =
      if (uid == null) {
        // Case 2 and 4
        // Get all the public datasets by default
        DATASET.IS_PUBLIC.eq(true)
      } else {
        if (includePublic) {
          // Case 3
          // Get all the datasets that `uid` has access to and the public datasets
          DATASET.IS_PUBLIC.eq(true).or(DATASET_USER_ACCESS.UID.isNotNull)
        } else {
          // Case 1
          // If `includePublic` is false get only user accessible datasets
          DATASET_USER_ACCESS.UID.isNotNull
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
      DATASET.CREATION_TIME
    )
      .and(getContainsFilter(params.datasetIds, DATASET.DID))
      .and(
        getFullTextSearchFilter(splitKeywords, List(DATASET.NAME, DATASET.DESCRIPTION))
      )
  }

  override protected def getGroupByFields: Seq[GroupField] = {
    Seq.empty
  }

  override protected def toEntryImpl(
      uid: Integer,
      record: Record
  ): DashboardResource.DashboardClickableFileEntry = {
    val dataset = record.into(DATASET).into(classOf[Dataset])
    val owner = record.into(USER).into(classOf[User])
    var size = 0L

    try {
      size = LakeFSStorageClient.retrieveRepositorySize(dataset.getRepositoryName)
    } catch {
      case e: io.lakefs.clients.sdk.ApiException =>
        // Treat all LakeFS ApiException as mismatch (repository not found, being deleted, or any fatal error)
        logger.error(
          s"LakeFS ApiException for dataset repository '${dataset.getRepositoryName}': ${e.getMessage}",
          e
        )
        return null
    }

    val dd = DashboardDataset(
      dataset,
      owner.getEmail,
      Option(
        record.get(
          DATASET_USER_ACCESS.PRIVILEGE,
          classOf[PrivilegeEnum]
        )
      ).getOrElse(PrivilegeEnum.NONE),
      dataset.getOwnerUid == uid,
      size
    )
    DashboardClickableFileEntry(
      resourceType = SearchQueryBuilder.DATASET_RESOURCE_TYPE,
      dataset = Some(dd)
    )
  }
}

class DatasetSearchQueryBuilder {}
