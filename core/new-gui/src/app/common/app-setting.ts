import { isDevMode } from '@angular/core';

export class AppSettings {
  private static readonly SERVER_ADDRESS = 'http://localhost';
  private static readonly API_PORT = 8080;
  private static readonly API_ENDPOINT = 'api';

  public static getApiEndpoint(): string {
    if (isDevMode()) {
      // if we are in development mode (we use angular CLI development server at localhost:4200)
      // we need to send request to localhost:8080 api endpoint
      return `${AppSettings.SERVER_ADDRESS}:${AppSettings.API_PORT}/${AppSettings.API_ENDPOINT}`;
    } else {
      // if we are in production mode (the frontend page is served by backend at a server)
      // we use relative path to resolve the URL
      return AppSettings.API_ENDPOINT;
    }
  }
}
