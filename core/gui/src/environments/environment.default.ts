// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// The file contains the default environment template
// it's used to store app settings and flags to turn on or off different features

export const defaultEnvironment = {
  /**
   * whether we are in production mode, default is false
   */
  production: false,

  /**
   * root API URL of the backend
   */
  apiUrl: "api",

  /**
   * whether export execution result is supported
   */
  exportExecutionResultEnabled: false,

  /**
   * whether automatically correcting attribute name on change is enabled
   * see AutoAttributeCorrectionService for more details
   */
  autoAttributeCorrectionEnabled: true,

  /**
   * whether user system is enabled
   */
  userSystemEnabled: false,

  /**
   * whether selecting files from datasets instead of the local file system.
   * The user system must be enabled to make this flag work!
   */
  selectingFilesFromDatasetsEnabled: true,

  /**
   * whether local login is enabled
   */
  localLogin: true,

  /**
   * whether google login is enabled
   */
  googleLogin: true,
  /**
   * whether invite only is enabled
   */
  inviteOnly: false,

  /**
   * whether user preset feature is enabled, requires user system to be enabled
   */
  userPresetEnabled: false,

  /**
   * whether workflow executions tracking feature is enabled
   */
  workflowExecutionsTrackingEnabled: false,

  /**
   * whether linkBreakpoint is supported
   */
  linkBreakpointEnabled: true,

  /**
   * whether rendering jointjs components asynchronously
   */
  asyncRenderingEnabled: false,

  /**
   * whether time-travel is enabled
   */
  timetravelEnabled: false,

  /**
   * Whether to connect to local or production shared editing server. Set to true if you have
   * reverse proxy set up for y-websocket.
   */
  productionSharedEditingServer: false,

  /**
   * the file size limit for dataset upload
   */
  singleFileUploadMaximumSizeMB: 20,

  /**
   * the maximum number of file chunks that can be held in the memory;
   * you may increase this number if your deployment environment has enough memory resource.
   */
  maxNumberOfConcurrentUploadingFileChunks: 10,

  /**
   * the size of each chunk during the multipart upload of file
   */
  multipartUploadChunkSizeByte: 50 * 1024 * 1024, // 50 MB

  /**
   * default data transfer batch size for workflows
   */
  defaultDataTransferBatchSize: 400,

  /**
   * whether to send email notification when workflow execution is completed/failed/paused/killed
   */
  workflowEmailNotificationEnabled: false,

  /**
   * whether hub feature is enabled
   */
  hubEnabled: true,

  /**
   * whether forum feature is enabled
   */
  forumEnabled: false,

  /**
   * whether project feature is enabled
   */
  projectEnabled: false,

  /**
   * Can be configured as { username: "texera", password: "password" }
   * If configured, this will be automatically filled into the local login input box
   */
  defaultLocalUser: {} as { username?: string; password?: string },

  /**
   * maximum number of console messages to store per operator
   */
  operatorConsoleMessageBufferSize: 100,
};

export type AppEnv = typeof defaultEnvironment;
