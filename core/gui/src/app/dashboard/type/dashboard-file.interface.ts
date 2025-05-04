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

export interface DashboardFile
  extends Readonly<{
    ownerEmail: string;
    accessLevel: string;
    file: UserFile;
  }> {}

/**
 * This interface stores the information about the users' files.
 * These information is used to locate the file for the operators.
 * Corresponds to `src/main/scala/edu/uci/ics/texera/web/resource/dashboard/file/UserFileResource.scala` (backend);
 * and `core/scripts/sql/texera_ddl.sql`, table `file` (database).
 */
export interface UserFile {
  ownerUid: number;
  fid: number;
  size: number;
  name: string;
  path: string;
  description: string;
  uploadTime: number;
}

/**
 * This interface stores the information about the users' files when uploading.
 * These information is used to upload the file to the backend.
 */
export interface FileUploadItem {
  file: File;
  name: string;
  description: string;
  uploadProgress: number;
  isUploadingFlag: boolean;
}

/**
 * This enum type helps indicate the method in which DashboardUserFileEntry[] is sorted
 */
export enum SortMethod {
  NameAsc,
  NameDesc,
  SizeDesc,
  UploadTimeAsc,
  UploadTimeDesc,
}
