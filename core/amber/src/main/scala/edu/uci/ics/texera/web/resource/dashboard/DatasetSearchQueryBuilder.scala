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

import edu.uci.ics.amber.core.storage.util.LakeFSStorageClient
import edu.uci.ics.texera.dao.jooq.generated.Tables.{DATASET, DATASET_USER_ACCESS}
import edu.uci.ics.texera.dao.jooq.generated.enums.PrivilegeEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.User.USER
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{Dataset, User}
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.DashboardClickableFileEntry
import edu.uci.ics.texera.web.resource.dashboard.FulltextSearchQueryUtils.{
  getContainsFilter,
  getDateFilter,
  getFullTextSearchFilter
}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource.DashboardDataset
import org.jooq.impl.DSL
import org.jooq.{Condition, GroupField, Record, TableLike}
import com.typesafe.scalalogging.LazyLogging
import scala.jdk.CollectionConverters.CollectionHasAsScala

object DatasetSearchQueryBuilder extends SearchQueryBuilder with LazyLogging {
  override protected val mappedResourceSchema: UnifiedResourceSchema = UnifiedResourceSchema(
    resourceType = DSL.inline(SearchQueryBuilder.DATASET_RESOURCE_TYPE),
    name = DATASET.NAME,
    description = DATASET.DESCRIPTION,
    creationTime = DATASET.CREATION_TIME,
    did = DATASET.DID,
    ownerId = DATASET.OWNER_UID,
    isDatasetPublic = DATASET.IS_PUBLIC,
    datasetUserAccess = DATASET_USER_ACCESS.PRIVILEGE
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
    val baseJoin = DATASET
      .leftJoin(DATASET_USER_ACCESS)
      .on(DATASET_USER_ACCESS.DID.eq(DATASET.DID))
      .leftJoin(USER)
      .on(USER.UID.eq(DATASET.OWNER_UID))

    // Default condition starts as true, ensuring all datasets are selected initially.
    var condition: Condition = DSL.trueCondition()

    if (uid == null) {
      // If `uid` is null, the user is not logged in or performing a public search
      // We only select datasets marked as public
      condition = DATASET.IS_PUBLIC.eq(true)
    } else {
      // When `uid` is present, we add a condition to only include datasets with direct user access.
      val userAccessCondition = DATASET_USER_ACCESS.UID.eq(uid)

      if (includePublic) {
        // If `includePublic` is true, we extend visibility to public datasets as well.
        condition = userAccessCondition.or(DATASET.IS_PUBLIC.eq(true))
      } else {
        condition = userAccessCondition
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
      size = LakeFSStorageClient.retrieveRepositorySize(dataset.getName)
    } catch {
      case e: io.lakefs.clients.sdk.ApiException =>
        // Treat all LakeFS ApiException as mismatch (repository not found, being deleted, or any fatal error)
        logger.error(s"LakeFS ApiException for dataset '${dataset.getName}': ${e.getMessage}", e)
        return null
    }

    val dd = DashboardDataset(
      dataset,
      owner.getEmail,
      record.get(DATASET_USER_ACCESS.PRIVILEGE, classOf[PrivilegeEnum]),
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
