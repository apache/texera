import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { interval, Observable, Subscription } from "rxjs";
import { AppSettings } from "../../app-setting";
import { User, Role } from "../../type/user";
import { timer } from "rxjs";
import { startWith, ignoreElements } from "rxjs/operators";
import { JwtHelperService } from "@auth0/angular-jwt";
import { NotificationService } from "../notification/notification.service";
import { environment } from "../../../../environments/environment";

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
export class AuthService {
  private inviteOnly = environment.inviteOnly;
  public static readonly LOGIN_ENDPOINT = "auth/login";
  public static readonly REFRESH_TOKEN = "auth/refresh";
  public static readonly REGISTER_ENDPOINT = "auth/register";
  public static readonly GOOGLE_LOGIN_ENDPOINT = "auth/google/login";

  private tokenExpirationSubscription?: Subscription;
  private refreshTokenSubscription?: Subscription;

  constructor(
    private http: HttpClient,
    private jwtHelperService: JwtHelperService,
    private notificationService: NotificationService
  ) {}

  /**
   * This method will handle the request for user registration.
   * It will automatically login, save the user account inside and trigger userChangeEvent when success
   * @param username
   * @param password
   */
  public register(username: string, password: string): Observable<Readonly<{ accessToken: string }>> {
    return this.http.post<Readonly<{ accessToken: string }>>(
      `${AppSettings.getApiEndpoint()}/${AuthService.REGISTER_ENDPOINT}`,
      {
        username,
        password,
      }
    );
  }

  /**
   * This method will handle the request for Google login.
   * It will automatically login, save the user account inside and trigger userChangeEvent when success

   */
  public googleAuth(credential: string): Observable<Readonly<{ accessToken: string }>> {
    return this.http.post<Readonly<{ accessToken: string }>>(
      `${AppSettings.getApiEndpoint()}/${AuthService.GOOGLE_LOGIN_ENDPOINT}`,
      `${credential}`
    );
  }

  /**
   * This method will handle the request for user login.
   * It will automatically login, save the user account inside and trigger userChangeEvent when success
   * @param username
   * @param password
   */
  public auth(username: string, password: string): Observable<Readonly<{ accessToken: string }>> {
    return this.http.post<Readonly<{ accessToken: string }>>(
      `${AppSettings.getApiEndpoint()}/${AuthService.LOGIN_ENDPOINT}`,
      { username, password }
    );
  }

  /**
   * this method will clear the saved user account and trigger userChangeEvent
   */
  public logout(): undefined {
    AuthService.removeAccessToken();
    this.tokenExpirationSubscription?.unsubscribe();
    this.refreshTokenSubscription?.unsubscribe();
    return undefined;
  }

  public loginWithExistingToken(): User | undefined {
    this.tokenExpirationSubscription?.unsubscribe();
    const token = AuthService.getAccessToken();

    if (token == null) {
      return this.logout();
    }

    if (this.jwtHelperService.isTokenExpired(token)) {
      this.notificationService.error("Access token is expired!");
      return this.logout();
    }

    const role = this.jwtHelperService.decodeToken(token).role;
    const email = this.jwtHelperService.decodeToken(token).email;
    if (this.inviteOnly && role == Role.INACTIVE) {
      alert("The account request of " + email + " is received and pending.");
      return this.logout();
    }

    this.registerAutoLogout();
    this.registerAutoRefreshToken();
    return {
      uid: this.jwtHelperService.decodeToken(token).userId,
      name: this.jwtHelperService.decodeToken(token).sub,
      email: email,
      googleId: this.jwtHelperService.decodeToken(token).googleId,
      googleAvatar: "https://lh3.googleusercontent.com/a/" + this.jwtHelperService.decodeToken(token).googleAvatar,
      role: role,
    };
  }

  /**
   * Refreshes the current accessToken to get a new accessToken
   * // TODO: for better security, use a separate refresh token to perform this refresh
   */
  private refreshToken(): Observable<Readonly<{ accessToken: string }>> {
    return this.http.post<Readonly<{ accessToken: string }>>(
      `${AppSettings.getApiEndpoint()}/${AuthService.REFRESH_TOKEN}`,
      { accessToken: AuthService.getAccessToken() }
    );
  }

  private registerAutoRefreshToken() {
    this.refreshTokenSubscription?.unsubscribe();
    this.refreshTokenSubscription = interval(TOKEN_REFRESH_INTERVAL_IN_MIN * 60 * 1000)
      .pipe(startWith(0)) // to trigger immediately for the first time.
      .subscribe(() => {
        this.refreshToken().subscribe(
          ({ accessToken }) => {
            AuthService.setAccessToken(accessToken);
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
    const token = AuthService.getAccessToken();
    if (token !== null && !this.jwtHelperService.isTokenExpired(token) && expirationTime !== undefined) {
      // use timer with ignoreElements to avoid event being immediately triggered (in RxJS 7)
      // see https://stackoverflow.com/questions/70013573/how-to-replicate-delay-from-rxjs-6-x
      this.tokenExpirationSubscription = timer(expirationTime - new Date().getTime())
        .pipe(ignoreElements())
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
