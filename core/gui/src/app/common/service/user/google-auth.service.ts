import { Injectable } from "@angular/core";
import { Subject } from "rxjs";
import { HttpClient } from "@angular/common/http";
import { AppSettings } from "../../app-setting";
declare var window: any;
export interface CredentialResponse {
  client_id: string;
  credential: string;
  select_by: string;
}
@Injectable({
  providedIn: "root",
})
export class GoogleAuthService {
  private _googleCredentialResponse = new Subject<CredentialResponse>();
  constructor(private http: HttpClient) {}
  public googleAuthInit(parent: HTMLElement | null) {
    this.waitForGoogleScript(2000)
      .then(() => {
        this.http.get(`${AppSettings.getApiEndpoint()}/auth/google/clientid`, { responseType: "text" }).subscribe({
          next: response => {
            window.google.accounts.id.initialize({
              client_id: response,
              callback: (auth: CredentialResponse) => {
                this._googleCredentialResponse.next(auth);
              },
            });
            window.google.accounts.id.renderButton(parent, { width: 200 });
            window.google.accounts.id.prompt();
          },
          error: (err: unknown) => {
            console.error(err);
          },
        });
      })
      .catch(err => {
        console.error(err.message);
      });
  }

  private waitForGoogleScript(timeout: number): Promise<void> {
    return new Promise((resolve, reject) => {
      const interval = 100;
      let elapsedTime = 0;

      const checkGoogleScript = setInterval(() => {
        elapsedTime += interval;

        if (typeof window.google !== "undefined") {
          clearInterval(checkGoogleScript);
          resolve();
        } else if (elapsedTime >= timeout) {
          clearInterval(checkGoogleScript);
          reject(new Error("Google script is not loaded or not available within the timeout period."));
        }
      }, interval);
    });
  }

  get googleCredentialResponse() {
    return this._googleCredentialResponse.asObservable();
  }
}
