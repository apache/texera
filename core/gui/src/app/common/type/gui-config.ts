// GUI configuration interface that matches the backend application.conf structure
export interface GuiConfig {
  apiUrl: string;
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
