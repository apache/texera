/**
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

/**
 * Addresses the file-upload endpoints of one versioned resource kind. Resource families differ only
 * in their base path and in the query-param name carrying the resource name, so the upload engine is
 * parameterized by this rather than duplicated per family.
 */
export interface FileResourceEndpoint {
  /** Path segment under the API root, e.g. "dataset". */
  readonly baseUrl: string;
  /** How to name this resource kind in user-facing copy, e.g. "dataset". */
  readonly label: string;
  /** Query-param name carrying the resource name, e.g. "datasetName". */
  readonly nameParamKey: string;
  /** site_settings key holding this resource's per-file upload ceiling, in MiB. */
  readonly maxFileSizeSettingKey: string;
  /** Fallback ceiling in MiB when that setting is absent or unparsable. */
  readonly defaultMaxFileSizeMiB: number;
  /** site_settings keys tuning this resource's multipart upload. */
  readonly chunkSizeSettingKey: string;
  readonly maxConcurrentChunksSettingKey: string;
  readonly maxConcurrentFilesSettingKey: string;
}

export const DATASET_FILE_RESOURCE_ENDPOINT: FileResourceEndpoint = {
  baseUrl: "dataset",
  label: "dataset",
  nameParamKey: "datasetName",
  maxFileSizeSettingKey: "dataset_single_file_upload_max_size_mib",
  defaultMaxFileSizeMiB: 20,
  chunkSizeSettingKey: "dataset_multipart_upload_chunk_size_mib",
  maxConcurrentChunksSettingKey: "dataset_max_number_of_concurrent_uploading_file_chunks",
  maxConcurrentFilesSettingKey: "dataset_max_number_of_concurrent_uploading_file",
};

export const MODEL_FILE_RESOURCE_ENDPOINT: FileResourceEndpoint = {
  baseUrl: "model",
  label: "model",
  nameParamKey: "modelName",
  maxFileSizeSettingKey: "model_single_file_upload_max_size_mib",
  defaultMaxFileSizeMiB: 2048,
  chunkSizeSettingKey: "model_multipart_upload_chunk_size_mib",
  maxConcurrentChunksSettingKey: "model_max_number_of_concurrent_uploading_file_chunks",
  maxConcurrentFilesSettingKey: "model_max_number_of_concurrent_uploading_file",
};
