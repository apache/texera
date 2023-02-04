import { Injectable } from "@angular/core";
import { Subject } from "rxjs";

@Injectable({
  providedIn: "root",
})
export class GoogleService {
  private _googleCredentialResponse = new Subject<google.accounts.id.CredentialResponse>();

  public googleInit() {
    google.accounts.id.initialize({
      client_id: "490139017655-ebp2i1bgj3jvm1buvdm2djqaaqhus763.apps.googleusercontent.com",
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
