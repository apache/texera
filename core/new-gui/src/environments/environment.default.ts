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
  apiUrl: 'api',
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
   * whether download execution result is supported
   */
  downloadExecutionResultEnabled: false,

  /**
   * whether user system is enabled
   */
  userSystemEnabled: false,

  amberEngineEnabled: true,

  /**
   * whether linkBreakpoint is supported
   */
  linkBreakpointEnabled: true,

  /**
   * the access code for mapbox
   */
  mapbox: {
    accessToken: ''
  },

  /**
   * all google-related configs
   */
  google: {
    clientID: ''
  }

};

export type AppEnv = typeof defaultEnvironment;

