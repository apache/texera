import { Injectable } from "@angular/core";
import { Subject } from "rxjs";

@Injectable({
  providedIn: "root",
})
export class GoogleService {
  private _googleCredentialResponse = new Subject<google.accounts.id.CredentialResponse>();

  public googleInit() {
    google.accounts.id.initialize({
      client_id: "490139017655-jp4qg7sism7ea5pcp2glpjv045qamt2r.apps.googleusercontent.com",
      callback: auth => {
        this._googleCredentialResponse.next(auth);
      },
    });
    google.accounts.id.prompt();
  }

  get googleCredentialResponse() {
    return this._googleCredentialResponse.asObservable();
  }
}
