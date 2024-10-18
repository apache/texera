import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { UserService } from "../../../../common/service/user/user.service";
import { AppSettings } from "../../../../common/app-setting";
import { catchError, map } from "rxjs/operators";
import { Observable, of } from "rxjs";

@Injectable({
  providedIn: "root",
})
export class FlarumService {
  constructor(
    private http: HttpClient,
    private userService: UserService
  ) {}
  public register() {
    return this.http.put(`${AppSettings.getApiEndpoint()}/discussion/register`, {});
  }

  auth() {
    const currentUser = this.userService.getCurrentUser();
    return this.http.post(
      "forum/api/token",
      { identification: currentUser!.email, password: currentUser!.googleId, remember: "1" },
      { headers: { "Content-Type": "application/json" }, withCredentials: true }
    );
  }

  checkForumHealth(): Observable<boolean> {
    return this.http
      .get<{ forumAvailable: boolean }>(`${AppSettings.getApiEndpoint()}/dashboard/forumHealth`)
      .pipe(
        map((response) => {
          // console.log("Health check response:", response);
          return response.forumAvailable;
        }),
        catchError((error: unknown) => {
          // console.warn("Forum health check failed:", error);
          return of(false);
        })
      );
  }
}
