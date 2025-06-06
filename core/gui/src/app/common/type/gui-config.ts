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

// Please refer to core/config/src/main/resources/gui.conf for the definition of each config item
export interface GuiConfig {
  exportExecutionResultEnabled: boolean;
  autoAttributeCorrectionEnabled: boolean;
  userSystemEnabled: boolean;
  selectingFilesFromDatasetsEnabled: boolean;
  localLogin: boolean;
  googleLogin: boolean;
  inviteOnly: boolean;
  userPresetEnabled: boolean;
  workflowExecutionsTrackingEnabled: boolean;
  linkBreakpointEnabled: boolean;
  asyncRenderingEnabled: boolean;
  timetravelEnabled: boolean;
  productionSharedEditingServer: boolean;
  pythonLanguageServerPort: string;
  singleFileUploadMaximumSizeMB: number;
  maxNumberOfConcurrentUploadingFileChunks: number;
  multipartUploadChunkSizeByte: number;
  defaultDataTransferBatchSize: number;
  workflowEmailNotificationEnabled: boolean;
  hubEnabled: boolean;
  forumEnabled: boolean;
  projectEnabled: boolean;
  operatorConsoleMessageBufferSize: number;
  defaultLocalUser?: { username?: string; password?: string };
}
