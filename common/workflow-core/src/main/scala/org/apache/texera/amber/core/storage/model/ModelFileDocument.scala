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
import org.apache.texera.amber.core.storage.model.ModelFileDocument.fileServiceGetModelPresignURLEndpoint

import java.net.URI

object ModelFileDocument {
  // The endpoint of getting a presigned url for a model file from the file service.
  lazy val fileServiceGetModelPresignURLEndpoint: String =
    sys.env
      .getOrElse(
        EnvironmentalVariable.ENV_FILE_SERVICE_GET_MODEL_PRESIGNED_URL_ENDPOINT,
        "http://localhost:9092/api/model/presign-download"
      )
      .trim
}

/**
  * A read-only document over a single file in a model's LakeFS repository (`model-{mid}`),
  * addressed by a `model:///{repositoryName}/{versionHash}/{fileRelativePath}` URI.
  */
private[storage] class ModelFileDocument(uri: URI)
    extends LakeFSFileDocument(uri, fileServiceGetModelPresignURLEndpoint)
