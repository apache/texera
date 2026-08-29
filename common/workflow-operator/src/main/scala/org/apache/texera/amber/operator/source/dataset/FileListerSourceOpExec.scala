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

package org.apache.texera.amber.operator.source.dataset

import org.apache.texera.amber.core.executor.SourceOperatorExecutor
import org.apache.texera.amber.core.storage.ResourceType
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.amber.core.tuple.TupleLike
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.tables.Dataset.DATASET
import org.apache.texera.dao.jooq.generated.tables.DatasetVersion.DATASET_VERSION
import org.apache.texera.dao.jooq.generated.tables.User.USER

object FileListerSourceOpExec {

  /**
    * Parses a dataset version path (/dataset/ownerEmail/datasetName/versionName) into its
    * (resourceTypePrefix, ownerEmail, datasetName, versionName) components.
    *
    * @throws IllegalArgumentException if the path is not a well-formed dataset version path
    */
  private[dataset] def parseDatasetVersionPath(
      datasetVersionPath: String
  ): (String, String, String, String) = {
    val segments = datasetVersionPath.split("/").filter(_.nonEmpty)
    val invalidPath = s"Invalid dataset version path '$datasetVersionPath'; " +
      "expected /dataset/ownerEmail/datasetName/versionName"

    if (segments.headOption.exists(ResourceType.isValidPrefix)) {
      require(segments.length >= 4, invalidPath)
      (segments(0), segments(1), segments(2), segments(3))
    } else {
      require(segments.length >= 3, invalidPath)
      (ResourceType.Dataset.toString, segments(0), segments(1), segments(2))
    }
  }

  private[dataset] def canonicalVersionPath(
      resourceTypePrefix: String,
      ownerEmail: String,
      datasetName: String,
      versionName: String
  ): String = s"/$resourceTypePrefix/$ownerEmail/$datasetName/$versionName"
}

class FileListerSourceOpExec private[dataset] (descString: String) extends SourceOperatorExecutor {
  private val desc: FileListerSourceOpDesc =
    objectMapper.readValue(descString, classOf[FileListerSourceOpDesc])

  override def produceTuple(): Iterator[TupleLike] = {
    val (resourceTypePrefix, ownerEmail, datasetName, versionName) =
      FileListerSourceOpExec.parseDatasetVersionPath(desc.datasetVersionPath)

    val versionPath = FileListerSourceOpExec.canonicalVersionPath(
      resourceTypePrefix,
      ownerEmail,
      datasetName,
      versionName
    )

    val (repositoryName, versionHash) =
      SqlServer
        .getInstance()
        .createDSLContext()
        .select(DATASET.REPOSITORY_NAME, DATASET_VERSION.VERSION_HASH)
        .from(DATASET)
        .join(USER)
        .on(USER.UID.eq(DATASET.OWNER_UID))
        .join(DATASET_VERSION)
        .on(DATASET_VERSION.DID.eq(DATASET.DID))
        .where(USER.EMAIL.eq(ownerEmail))
        .and(DATASET.NAME.eq(datasetName))
        .and(DATASET_VERSION.NAME.eq(versionName))
        .fetchOne(r => (r.value1(), r.value2()))

    LakeFSStorageClient
      .retrieveObjectsOfVersion(repositoryName, versionHash)
      .map(obj => TupleLike("filename" -> s"$versionPath/${obj.getPath}"))
      .iterator
  }
}
