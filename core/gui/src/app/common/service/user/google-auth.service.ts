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
    this.http.get(`${AppSettings.getApiEndpoint()}/auth/google/clientid`, { responseType: "text" }).subscribe({
      next: response => {
        window.onGoogleLibraryLoad = () => {
          window.google.accounts.id.initialize({
            client_id: response,
            callback: (auth: CredentialResponse) => {
              this._googleCredentialResponse.next(auth);
            },
          });
          window.google.accounts.id.renderButton(parent, { width: 200 });
          window.google.accounts.id.prompt();
        };
      },
      error: (err: unknown) => {
        console.error(err);
      },
    });
  }

  get googleCredentialResponse() {
    return this._googleCredentialResponse.asObservable();
  }
}
