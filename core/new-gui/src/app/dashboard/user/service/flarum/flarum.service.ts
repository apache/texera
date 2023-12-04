import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { UserService } from "../../../../common/service/user/user.service";
import { AppSettings } from "../../../../common/app-setting";

@Injectable({
  providedIn: "root",
})
export class FlarumService {
  constructor(private http: HttpClient, private userService: UserService) {}
  public register() {
    return this.http.put(`${AppSettings.getApiEndpoint()}/discussion/register`, {});
  }

  auth() {
    const currentUser = this.userService.getCurrentUser();
    return this.http.post(
      "http://localhost:8888/api/token",
      { identification: currentUser!.email, password: currentUser!.googleId, remember: "1" },
      { headers: { "Content-Type": "application/json" }, withCredentials: true }
    );
  }
}
