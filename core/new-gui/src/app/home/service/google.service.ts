import { Injectable } from "@angular/core";
import { Subject } from "rxjs";
import { environment } from "../../../environments/environment";
declare var window: any;

export interface credentialRes {
  client_id: string;
  credential: string;
  select_by: string;
}
@Injectable({
  providedIn: "root",
})
export class GoogleService {
  private _googleCredentialResponse = new Subject<credentialRes>();

  public googleInit(parent: HTMLElement | null) {
    window.onGoogleLibraryLoad = () => {
      window.google.accounts.id.initialize({
        client_id: environment.google.clientID,
        callback: (auth: credentialRes) => {
          this._googleCredentialResponse.next(auth);
        },
      });
      window.google.accounts.id.renderButton(parent, { width: "270" });
      window.google.accounts.id.prompt();
    };
  }

  get googleCredentialResponse() {
    return this._googleCredentialResponse.asObservable();
  }
}
