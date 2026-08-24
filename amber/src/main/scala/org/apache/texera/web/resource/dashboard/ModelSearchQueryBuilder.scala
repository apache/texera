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

import org.apache.texera.dao.jooq.generated.Tables.{MODEL, MODEL_USER_ACCESS}
import org.jooq.impl.DSL

/** Query logic lives in [[VersionedResourceSearchQueryBuilder]]; only the projection is here. */
object ModelSearchQueryBuilder
    extends VersionedResourceSearchQueryBuilder(VersionedResourceTables.ModelTables) {

  override protected val mappedResourceSchema: UnifiedResourceSchema = UnifiedResourceSchema(
    resourceType = DSL.inline(SearchQueryBuilder.MODEL_RESOURCE_TYPE),
    name = MODEL.NAME,
    description = MODEL.DESCRIPTION,
    creationTime = MODEL.CREATION_TIME,
    ownerId = MODEL.OWNER_UID,
    versionedResourceId = MODEL.MID,
    repositoryName = MODEL.REPOSITORY_NAME,
    isVersionedResourcePublic = MODEL.IS_PUBLIC,
    isVersionedResourceDownloadable = MODEL.IS_DOWNLOADABLE,
    versionedResourceUserAccess = MODEL_USER_ACCESS.PRIVILEGE,
    versionedResourceCoverImage = MODEL.COVER_IMAGE,
    modelFramework = MODEL.FRAMEWORK,
    modelFormat = MODEL.FORMAT
  )
}

class ModelSearchQueryBuilder {}
