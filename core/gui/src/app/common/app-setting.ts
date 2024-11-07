import { environment } from "../../environments/environment";

export class AppSettings {
  public static getApiEndpoint(): string {
    return environment.apiUrl;
  }

  public static getTexeraApiEndpoint(): string {
    return environment.texeraApiUrl;
  }
}
