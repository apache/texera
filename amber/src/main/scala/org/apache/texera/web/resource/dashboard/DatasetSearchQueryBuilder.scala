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

import org.apache.texera.dao.jooq.generated.Tables.{DATASET, DATASET_USER_ACCESS}
import org.jooq.impl.DSL

/** Query logic lives in [[VersionedResourceSearchQueryBuilder]]; only the projection is here. */
object DatasetSearchQueryBuilder
    extends VersionedResourceSearchQueryBuilder(VersionedResourceTables.DatasetTables) {

  override protected val mappedResourceSchema: UnifiedResourceSchema = UnifiedResourceSchema(
    resourceType = DSL.inline(SearchQueryBuilder.DATASET_RESOURCE_TYPE),
    name = DATASET.NAME,
    description = DATASET.DESCRIPTION,
    creationTime = DATASET.CREATION_TIME,
    ownerId = DATASET.OWNER_UID,
    versionedResourceId = DATASET.DID,
    repositoryName = DATASET.REPOSITORY_NAME,
    isVersionedResourcePublic = DATASET.IS_PUBLIC,
    isVersionedResourceDownloadable = DATASET.IS_DOWNLOADABLE,
    versionedResourceUserAccess = DATASET_USER_ACCESS.PRIVILEGE,
    versionedResourceCoverImage = DATASET.COVER_IMAGE
  )
}

class DatasetSearchQueryBuilder {}
