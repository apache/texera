import { Injectable } from "@angular/core";
import { Subject } from "rxjs";
import { HttpClient } from "@angular/common/http";
import { AppSettings } from "../../common/app-setting";
declare var window: any;

@Injectable({
  providedIn: "root",
})
export class GoogleLoginService {
  private _googleCredentialResponse = new Subject<any>();
  constructor(private http: HttpClient) {}
  public googleInit(parent: HTMLElement | null) {
    this.http.get(`${AppSettings.getApiEndpoint()}/auth/google/clientid`, { responseType: "text" }).subscribe({
      next: response => {
        window.onGoogleLibraryLoad = () => {
          window.google.accounts.id.initialize({
            client_id: response,
            callback: (auth: any) => {
              this._googleCredentialResponse.next(auth);
            },
          });
          window.google.accounts.id.renderButton(parent, { width: "270" });
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
