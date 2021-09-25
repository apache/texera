import { Injectable } from "@angular/core";
import { Observable, ReplaySubject } from "rxjs";
import { User } from "../../type/user";
import { GoogleAuthService } from "ng-gapi";
import { AuthService } from "./auth.service";
import { environment } from "../../../../environments/environment";
import { map } from "rxjs/operators";

/**
 * User Service contains the function of registering and logging the user.
 * It will save the user account inside for future use.
 *
 * @author Adam
 */
@Injectable({
  providedIn: "root",
})
export class UserService {
  private currentUser?: User = undefined;
  private userChangeSubject: ReplaySubject<User | undefined> = new ReplaySubject<User | undefined>(1);

  constructor(private googleAuthService: GoogleAuthService, private authService: AuthService) {
    if (environment.userSystemEnabled) {
      this.authService.loginFromSession().subscribe(user => this.changeUser(user));
    }
  }

  public login(username: string, password: string): Observable<void> {
    // validate the credentials with backend
    return this.authService.auth(username, password).pipe(map(res => this.handleAccessToken(res.accessToken)));
  }

  public googleLogin(): Observable<void> {
    return this.googleAuthService.getAuth().pipe(
      map(Auth => {
        // grantOfflineAccess allows application to access specified scopes offline
        Auth.grantOfflineAccess().then(({ code }) =>
          this.authService.googleAuth(code).subscribe(res => this.handleAccessToken(res.accessToken))
        );
      })
    );
  }

  public isLogin(): boolean {
    return this.currentUser !== undefined;
  }

  public userChanged(): Observable<User | undefined> {
    return this.userChangeSubject.asObservable();
  }

  public logout(): void {
    this.authService.logout().subscribe(_ => this.changeUser(undefined));
  }

  public register(username: string, password: string): Observable<void> {
    return this.authService.register(username, password).pipe(map(res => this.handleAccessToken(res.accessToken)));
  }

  /**
   * changes the current user and triggers currentUserSubject
   * @param user
   */
  private changeUser(user: User | undefined): void {
    this.currentUser = user;
    this.userChangeSubject.next(this.currentUser);
  }

  private handleAccessToken(accessToken: string) {
    AuthService.setAccessToken(accessToken);
    this.authService.loginFromSession().subscribe(user => this.changeUser(user));
  }

  /**
   * check the given parameter is legal for login/registration
   * @param username
   */
  public static validateUsername(username: string): { result: boolean; message: string } {
    if (username.trim().length === 0) {
      return { result: false, message: "Username should not be empty." };
    }
    return { result: true, message: "Username frontend validation success." };
  }
}
