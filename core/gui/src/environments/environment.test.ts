import { AppEnv, defaultEnvironment } from "./environment.default";

export const environment: AppEnv = {
  ...defaultEnvironment,
  userSystemEnabled: true,
};
