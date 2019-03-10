import { AppEnv, defaultEnvironment } from './environment.default';

export const environment: AppEnv = {
  ...defaultEnvironment,
  production: true,
  /**
   * if we are in production mode (the frontend page is served by backend at a server)
   * we use relative path to resolve the URL
   * if app is deployed at a server, this can be replaced by the server address
   */
  apiUrl: 'api',
};
