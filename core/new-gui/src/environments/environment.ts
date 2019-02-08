// The file contents for the current environment will overwrite these during build.
// The build system defaults to the dev environment which uses `environment.ts`, but if you do
// `ng build --env=prod` then `environment.prod.ts` will be used instead.
// The list of which env maps to which file can be found in `.angular-cli.json`.

import { AppEnv, defaultEnvironment } from './environment.default';

export const environment: AppEnv = {
  ...defaultEnvironment,
  /**
   * if we are in development mode (we use angular CLI development server at localhost:4200)
   * we need to send request to localhost:8080 api endpoint
   */
  apiUrl: 'localhost:8080/api'
};
