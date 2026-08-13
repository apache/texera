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

package org.apache.texera.amber.core.storage.model

import org.apache.texera.common.config.EnvironmentalVariable
import org.apache.texera.amber.core.storage.model.DatasetFileDocument.fileServiceGetPresignURLEndpoint
import org.apache.texera.amber.core.storage.util.dataset.GitVersionControlLocalFileStorage

import java.net.URI
import java.nio.file.Path

object DatasetFileDocument {
  // The endpoint of getting presigned url from the file service, also stored in the environment vars.
  lazy val fileServiceGetPresignURLEndpoint: String =
    sys.env
      .getOrElse(
        EnvironmentalVariable.ENV_FILE_SERVICE_GET_DATASET_PRESIGNED_URL_ENDPOINT,
        "http://localhost:9092/api/dataset/presign-download"
      )
      .trim
}

private[storage] class DatasetFileDocument(uri: URI)
    extends LakeFSFileDocument(uri, fileServiceGetPresignURLEndpoint) {

  override def clear(): Unit = {
    // first remove the temporary file (handled by the shared base)
    super.clear()

    lazy val datasetsRootPath =
      Path
        .of(sys.env.getOrElse("TEXERA_HOME", "."))
        .resolve("amber")
        .resolve("user-resources")
        .resolve("datasets")

    def getDatasetPath(did: Integer): Path = {
      datasetsRootPath.resolve(did.toString)
    }

    // then remove the dataset file from the local git-backed storage
    GitVersionControlLocalFileStorage.removeFileFromRepo(
      getDatasetPath(0),
      getDatasetPath(0).resolve(fileRelativePath)
    )
  }
}
