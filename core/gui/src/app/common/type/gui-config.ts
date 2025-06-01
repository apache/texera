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

// GUI configuration interface that matches the backend application.conf structure
export interface GuiConfig {
  /**
   * root API URL of the backend
   */
  apiUrl: string;

  /**
   * whether export execution result is supported
   */
  exportExecutionResultEnabled: boolean;

  /**
   * whether automatically correcting attribute name on change is enabled
   * see AutoAttributeCorrectionService for more details
   */
  autoAttributeCorrectionEnabled: boolean;

  /**
   * whether user system is enabled
   */
  userSystemEnabled: boolean;

  /**
   * whether selecting files from datasets instead of the local file system.
   * The user system must be enabled to make this flag work!
   */
  selectingFilesFromDatasetsEnabled: boolean;

  /**
   * whether local login is enabled
   */
  localLogin: boolean;

  /**
   * whether google login is enabled
   */
  googleLogin: boolean;

  /**
   * whether invite only is enabled
   */
  inviteOnly: boolean;

  /**
   * whether user preset feature is enabled, requires user system to be enabled
   */
  userPresetEnabled: boolean;

  /**
   * whether workflow executions tracking feature is enabled
   */
  workflowExecutionsTrackingEnabled: boolean;

  /**
   * whether linkBreakpoint is supported
   */
  linkBreakpointEnabled: boolean;

  /**
   * whether rendering jointjs components asynchronously
   */
  asyncRenderingEnabled: boolean;

  /**
   * whether time-travel is enabled
   */
  timetravelEnabled: boolean;

  /**
   * Whether to connect to local or production shared editing server. Set to true if you have
   * reverse proxy set up for y-websocket.
   */
  productionSharedEditingServer: boolean;

  /**
   * The port of the python language server. If not set, no port will be used in the final url
   */
  pythonLanguageServerPort: string;

  /**
   * the file size limit for dataset upload
   */
  singleFileUploadMaximumSizeMB: number;

  /**
   * the maximum number of file chunks that can be held in the memory;
   * you may increase this number if your deployment environment has enough memory resource.
   */
  maxNumberOfConcurrentUploadingFileChunks: number;

  /**
   * the size of each chunk during the multipart upload of file
   */
  multipartUploadChunkSizeByte: number;

  /**
   * default data transfer batch size for workflows
   */
  defaultDataTransferBatchSize: number;

  /**
   * whether to send email notification when workflow execution is completed/failed/paused/killed
   */
  workflowEmailNotificationEnabled: boolean;

  /**
   * whether hub feature is enabled
   */
  hubEnabled: boolean;

  /**
   * whether forum feature is enabled
   */
  forumEnabled: boolean;

  /**
   * whether project feature is enabled
   */
  projectEnabled: boolean;

  /**
   * maximum number of console messages to store per operator
   */
  operatorConsoleMessageBufferSize: number;

  /**
   * Can be configured as { username: "texera", password: "password" }
   * If configured, this will be automatically filled into the local login input box
   */
  defaultLocalUser?: { username?: string; password?: string };
}
