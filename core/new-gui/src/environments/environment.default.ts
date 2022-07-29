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
   * whether fetching available source tables is enabled
   * see SourceTablesService for details
   */
  sourceTableEnabled: false,
  /**
   * whether operator schema propagation and autocomplete feature is enabled,
   * see SchemaPropagationService for details
   */
  schemaPropagationEnabled: true,
  /**
   * whether the backend support pause/resume functionality
   */
  pauseResumeEnabled: true,
  /**
   * whether the backend supports checking execution status
   */
  executionStatusEnabled: true,
  /**
   * whether export execution result is supported
   */
  exportExecutionResultEnabled: false,

  /**
   * whether user system is enabled
   */
  userSystemEnabled: false,

  /**
   * whether user preset feature is enabled, requires user system to be enabled
   */
  userPresetEnabled: false,

  /**
   * whether workflow executions tracking feature is enabled
   */
  workflowExecutionsTrackingEnabled: false,

  amberEngineEnabled: true,

  /**
   * whether linkBreakpoint is supported
   */
  linkBreakpointEnabled: true,

  /**
   * whether operator caching is enabled
   */
  operatorCacheEnabled: false,

  /**
   * whether debugger is enabled
   */
  debuggerEnabled: false,

  asyncRenderingEnabled: false,

  /**
   * the access code for mapbox
   */
  mapbox: {
    accessToken: "",
  },

  /**
   * all google-related configs
   */
  google: {
    clientID: "",
    publicKey: "",
  },

  /**
   * Whether workflow collab should be active
   */
  workflowCollabEnabled: false,

  /**
   * Default pagination values
   */
  defaultPageIndex: 1,
  defaultPageSize: 10,
  defaultNumOfItems: 0,
  defaultPageSizeOptions: [5, 10, 20, 30, 40],
};

export type AppEnv = typeof defaultEnvironment;
