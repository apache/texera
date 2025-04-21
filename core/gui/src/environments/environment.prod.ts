import { AppEnv, defaultEnvironment } from "./environment.default";

export const environment: AppEnv = {
  ...defaultEnvironment,
  production: true,

  userSystemEnabled: true,

  localLogin: true,

  inviteOnly: false,

  exportExecutionResultEnabled: true,

  userPresetEnabled: true,

  productionSharedEditingServer: true,

  asyncRenderingEnabled: true,

  workflowExecutionsTrackingEnabled: true,

  singleFileUploadMaximumSizeMB: 1024

};
