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

package edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils

import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.web.resource.dashboard.user.quota.UserQuotaResource.DatasetQuota

import scala.jdk.CollectionConverters._

object DatasetStatisticsUtils {
  final private lazy val context = SqlServer
    .getInstance()
    .createDSLContext()

  // this function retrieves the total counts of dataset that belongs to the user
  def getUserCreatedDatasetCount(uid: Integer): Int = {
    val count = context
      .selectCount()
      .from(DATASET)
      .where(DATASET.OWNER_UID.eq(uid))
      .fetchOne(0, classOf[Int])

    count
  }

  // this function would return a list of dataset ids that belongs to the user
  private def getUserCreatedDatasetList(uid: Integer): List[DatasetQuota] = {
    val result = context
      .select(
        DATASET.DID,
        DATASET.NAME,
        DATASET.CREATION_TIME
      )
      .from(DATASET)
      .where(DATASET.OWNER_UID.eq(uid))
      .fetch()

    result.asScala
      .map(record =>
        DatasetQuota(
          did = record.getValue(DATASET.DID),
          name = record.getValue(DATASET.NAME),
          creationTime = record.getValue(DATASET.CREATION_TIME).getTime,
          size = 0
        )
      )
      .toList
  }

  def getUserCreatedDatasets(uid: Integer): List[DatasetQuota] = {
    val datasetList = getUserCreatedDatasetList(uid)
    datasetList.map { dataset =>
      val size = 0 // we disabled the size calculation due to the switch of dataset implementation
      dataset.copy(size = size)
    }
  }
}
