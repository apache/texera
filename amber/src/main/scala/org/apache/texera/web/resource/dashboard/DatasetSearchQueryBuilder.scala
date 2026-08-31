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

import org.apache.texera.dao.jooq.generated.Tables.DATASET
import org.jooq.Field

/**
  * Query logic and projection live in [[VersionedResourceSearchQueryBuilder]]; only the columns
  * the [[VersionedResourceTables]] descriptor does not already name are here.
  */
object DatasetSearchQueryBuilder
    extends VersionedResourceSearchQueryBuilder(VersionedResourceTables.DatasetTables) {

  override protected val repositoryNameColumn: Field[String] = DATASET.REPOSITORY_NAME

  override protected val isDownloadableColumn: Field[java.lang.Boolean] = DATASET.IS_DOWNLOADABLE

  override protected val coverImageColumn: Field[String] = DATASET.COVER_IMAGE
}

class DatasetSearchQueryBuilder {}
