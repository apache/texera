import { environment } from "../../environments/environment";

export class AppSettings {
  public static getApiEndpoint(): string {
    return environment.apiUrl;
  }
}
