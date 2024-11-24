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
   * whether local login is enabled
   */
  localLogin: true,

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
   * default data transfer batch size for workflows
   */
  defaultDataTransferBatchSize: 400,

  /**
   * whether to send email notification when workflow execution is completed/failed/paused/killed
   */
  workflowEmailNotificationEnabled: false,
};

export type AppEnv = typeof defaultEnvironment;
