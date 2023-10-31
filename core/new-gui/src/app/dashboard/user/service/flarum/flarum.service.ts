import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { UserService } from "../../../../common/service/user/user.service";
import { Observable, of, throwError } from "rxjs";
import { switchMap, catchError } from "rxjs/operators";

@Injectable({
  providedIn: "root",
})
export class FlarumService {
  constructor(private http: HttpClient, private userService: UserService) {}

  createFlarumUser(username: string, identification: string, password: string): Observable<any> {
    const url = "http://localhost:80/api/users";
    const body = {
      data: {
        attributes: {
          username,
          email: identification,
          password,
        },
      },
    };
    const headers = {
      "Content-Type": "application/json",
      Authorization: `Token ${"http://localhost:80/api/token"}`,
    };
    return this.http.post(url, body, { headers }).pipe(
      catchError((error: unknown) => {
        console.error("Error creating Flarum user:", error);
        return throwError(error);
      })
    );
  }

  authenticateAndRedirect(): Observable<any> {
    const currentUser = this.userService.getCurrentUser();
    if (!currentUser) {
      console.error("No user is currently logged in");
      return of(null);
    }

    const username = currentUser.name.replace(/[^a-zA-Z0-9-]/g, "");
    const identification = currentUser.email;
    const password = "myPassword";

    return this.http
      .post(
        "http://localhost:80/public/api/token",
        { identification, password, remember: "1" },
        { headers: { "Content-Type": "application/json" }, withCredentials: true }
      )
      .pipe(
        switchMap((tokenResponse: any) => {
          if (tokenResponse.errors) {
            return this.createFlarumUser(username, identification, password).pipe(
              switchMap(response => {
                if (response && response.status === 200) {
                  return this.authenticateAndRedirect();
                }
                return of(null);
              })
            );
          }
          document.cookie = `flarum_remember=${tokenResponse.token};path=/`;
          return of(null);
        }),
        catchError((error: unknown) => {
          console.error("Error during authentication:", error);
          return throwError(error);
        })
      );
  }
}
