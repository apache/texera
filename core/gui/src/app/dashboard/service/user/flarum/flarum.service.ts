import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { UserService } from "../../../../common/service/user/user.service";

@Injectable({
  providedIn: "root",
})
export class FlarumService {
  constructor(
    private http: HttpClient,
    private userService: UserService
  ) {}

  register() {
    const user = this.userService.getCurrentUser();
    return this.http.post(
      "forum/api/users",
      {
        data: {
          attributes: { username: user!.email.split("@")[0] + user!.uid, email: user!.email, password: user!.googleId },
        },
      },
      { headers: { Authorization: "Token hdebsyxiigyklxgsqivyswwiisohzlnezzzzzzzz;userId=1" } }
    );
  }

  auth() {
    const user = this.userService.getCurrentUser();
    return this.http.post("forum/api/token", { identification: user!.email, password: user!.googleId, remember: "1" });
  }
}
