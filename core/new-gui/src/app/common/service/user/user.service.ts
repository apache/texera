import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { interval, Observable, of, ReplaySubject, Subscription } from "rxjs";
import { environment } from "../../../../environments/environment";
import { AppSettings } from "../../app-setting";
import { User } from "../../type/user";
import { GoogleAuthService } from "ng-gapi";
import { delay, filter, startWith } from "rxjs/operators";
import { JwtHelperService } from "@auth0/angular-jwt";

export const TOKEN_KEY = "access_token";
export const TOKEN_REFRESH_INTERVAL_IN_MIN = 15;

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
  public static readonly LOGIN_ENDPOINT = "auth/login";
  public static readonly REFRESH_TOKEN = "auth/refresh";
  public static readonly REGISTER_ENDPOINT = "auth/register";
  public static readonly GOOGLE_LOGIN_ENDPOINT = "auth/google-login";

  private currentUser: User | undefined = undefined;
  private userChangeSubject: ReplaySubject<User | undefined> = new ReplaySubject<User | undefined>(1);
  private tokenExpirationSubscription?: Subscription;
  private refreshTokenSubscription?: Subscription;
  private jwtHelperService = new JwtHelperService();

  constructor(private http: HttpClient, private googleAuth: GoogleAuthService) {
    if (environment.userSystemEnabled) {
      this.loginFromSession();
    }
  }

  /**
   * This method will handle the request for user registration.
   * It will automatically login, save the user account inside and trigger userChangeEvent when success
   * @param userName
   * @param password
   */
  public register(userName: string, password: string): Observable<Readonly<{ accessToken: string }>> {
    if (this.currentUser) {
      throw new Error("Already logged in when register.");
    }

    return this.http.post<Readonly<{ accessToken: string }>>(
      `${AppSettings.getApiEndpoint()}/${UserService.REGISTER_ENDPOINT}`,
      {
        userName,
        password,
      }
    );
  }

  /**
   * this method returns a Google OAuth Instance
   */
  public getGoogleAuthInstance(): Observable<gapi.auth2.GoogleAuth> {
    return this.googleAuth.getAuth();
  }

  /**
   * This method will handle the request for Google login.
   * It will automatically login, save the user account inside and trigger userChangeEvent when success
   * @param authCode string
   */
  public googleLogin(authCode: string): Observable<User> {
    if (this.currentUser) {
      throw new Error("Already logged in when login in.");
    }
    return this.http
      .post<User>(`${AppSettings.getApiEndpoint()}/${UserService.GOOGLE_LOGIN_ENDPOINT}`, { authCode })
      .pipe(filter((user: User) => user != null));
  }

  /**
   * This method will handle the request for user login.
   * It will automatically login, save the user account inside and trigger userChangeEvent when success
   * @param userName
   * @param password
   */
  public login(userName: string, password: string): Observable<Readonly<{ accessToken: string }>> {
    if (this.currentUser) {
      throw new Error("Already logged in when login in.");
    }
    return this.http.post<Readonly<{ accessToken: string }>>(
      `${AppSettings.getApiEndpoint()}/${UserService.LOGIN_ENDPOINT}`,
      { userName, password }
    );
  }

  /**
   * this method will clear the saved user account and trigger userChangeEvent
   */
  public logout(): void {
    UserService.removeAccessToken();
    this.changeUser(undefined);
    this.tokenExpirationSubscription?.unsubscribe();
    this.refreshTokenSubscription?.unsubscribe();
  }

  public getUser(): User | undefined {
    return this.currentUser;
  }

  public isLogin(): boolean {
    return this.currentUser !== undefined;
  }

  /**
   * changes the current user and triggers currentUserSubject
   * @param user
   */
  public changeUser(user: User | undefined): void {
    this.currentUser = user;
    this.userChangeSubject.next(this.currentUser);
  }

  /**
   * check the given parameter is legal for login/registration
   * @param userName
   */
  public validateUsername(userName: string): {
    result: boolean;
    message: string;
  } {
    if (userName.trim().length === 0) {
      return { result: false, message: "userName should not be empty" };
    }
    return { result: true, message: "userName frontend validation success" };
  }

  public userChanged(): Observable<User | undefined> {
    return this.userChangeSubject.asObservable();
  }

  public loginFromSession(): void {
    this.tokenExpirationSubscription?.unsubscribe();
    const token = UserService.getAccessToken();
    if (token !== null && !this.jwtHelperService.isTokenExpired(token)) {
      this.changeUser(<User>{ name: this.jwtHelperService.decodeToken(token).sub });
      this.registerAutoLogout();
      this.registerAutoRefreshToken();
    } else {
      // access token is expired, logout instantly
      this.logout();
    }
  }

  /**
   * Refreshes the current accessToken to get a new accessToken
   * // TODO: for better security, use a separate refresh token to perform this refresh
   */
  private refreshToken(): Observable<Readonly<{ accessToken: string }>> {
    return this.http.post<Readonly<{ accessToken: string }>>(
      `${AppSettings.getApiEndpoint()}/${UserService.REFRESH_TOKEN}`,
      { accessToken: UserService.getAccessToken() }
    );
  }

  private registerAutoRefreshToken() {
    this.refreshTokenSubscription?.unsubscribe();
    this.refreshTokenSubscription = interval(TOKEN_REFRESH_INTERVAL_IN_MIN * 60 * 1000)
      .pipe(startWith(0)) // to trigger immediately for the first time.
      .subscribe(() => {
        this.refreshToken().subscribe(
          ({ accessToken }) => {
            UserService.setAccessToken(accessToken);
            this.registerAutoLogout();
          },
          (_: unknown) => {
            // failed to refresh the access token, logout instantly.
            this.logout();
          }
        );
      });
  }

  private registerAutoLogout() {
    this.tokenExpirationSubscription?.unsubscribe();
    const expirationTime = this.jwtHelperService.getTokenExpirationDate()?.getTime();
    const token = UserService.getAccessToken();
    if (token !== null && !this.jwtHelperService.isTokenExpired(token) && expirationTime !== undefined) {
      this.tokenExpirationSubscription = of(null)
        .pipe(delay(expirationTime - new Date().getTime()))
        .subscribe(() => this.logout());
    }
  }

  static setAccessToken(token: string): void {
    localStorage.setItem(TOKEN_KEY, token);
  }

  static getAccessToken(): string | null {
    return localStorage.getItem(TOKEN_KEY);
  }

  static removeAccessToken(): void {
    localStorage.removeItem(TOKEN_KEY);
  }
}
