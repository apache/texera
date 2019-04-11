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
  sourceTableEnabled: true,
  /**
   * whether operator schema propagation and autocomplete feature is enabled,
   * see SchemaPropgationService for details
   */
  schemaPropagationEnabled: true,

  /**
   * whether the backend support pause/resume functionaility
   */
  pauseResumeEnabled: false
};

export type AppEnv = typeof defaultEnvironment;

