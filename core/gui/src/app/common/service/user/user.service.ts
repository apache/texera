import { Injectable } from "@angular/core";
import { Observable, of, ReplaySubject } from "rxjs";
import { Role, User } from "../../type/user";
import { AuthService } from "./auth.service";
import { environment } from "../../../../environments/environment";
import { catchError, map, shareReplay } from "rxjs/operators";

/**
 * User Service manages User information. It relies on different
 * auth services to authenticate a valid User.
 */
@Injectable({
  providedIn: "root",
})
export class UserService {
  private currentUser?: User = undefined;
  private userChangeSubject: ReplaySubject<User | undefined> = new ReplaySubject<User | undefined>(1);
  private cache = new Map<string, { url: string; expiry: number }>();
  private readonly cacheDuration = 3600 * 1000; // cache duration: 1h

  constructor(private authService: AuthService) {
    if (environment.userSystemEnabled) {
      const user = this.authService.loginWithExistingToken();
      this.changeUser(user);
    }
  }

  public getCurrentUser(): User | undefined {
    return this.currentUser;
  }

  public login(username: string, password: string): Observable<void> {
    // validate the credentials with backend
    return this.authService
      .auth(username, password)
      .pipe(map(({ accessToken }) => this.handleAccessToken(accessToken)));
  }

  public googleLogin(credential: string): Observable<void> {
    return this.authService.googleAuth(credential).pipe(map(({ accessToken }) => this.handleAccessToken(accessToken)));
  }

  public isLogin(): boolean {
    return this.currentUser !== undefined;
  }

  public isAdmin(): boolean {
    return this.currentUser?.role === Role.ADMIN;
  }

  public userChanged(): Observable<User | undefined> {
    return this.userChangeSubject.asObservable();
  }

  public logout(): void {
    this.authService.logout();
    this.changeUser(undefined);
  }

  public register(username: string, password: string): Observable<void> {
    return this.authService
      .register(username, password)
      .pipe(map(({ accessToken }) => this.handleAccessToken(accessToken)));
  }

  /**
   * changes the current user and triggers currentUserSubject
   * @param user
   */
  private changeUser(user: User | undefined): void {
    if (user) {
      const hue = Math.floor(Math.random() * 360); // Hue (0-360)
      const sat = Math.floor(60 + Math.random() * 20); // Saturation (60%-80%)
      const light = 50; // Lightness (50%)
      this.currentUser = { ...user, color: `hsl(${hue}, ${sat}%, ${light}%)` };
    } else {
      this.currentUser = user;
    }
    this.userChangeSubject.next(this.currentUser);
  }

  private handleAccessToken(accessToken: string): void {
    AuthService.setAccessToken(accessToken);
    this.changeUser(this.authService.loginWithExistingToken());
  }

  /**
   * check the given parameter is legal for login/registration
   * @param username
   */
  static validateUsername(username: string): { result: boolean; message: string } {
    if (username.trim().length === 0) {
      return { result: false, message: "Username should not be empty." };
    }
    return { result: true, message: "Username frontend validation success." };
  }

  getAvatar(googleAvatar: string): Observable<string | undefined> {
    if (!googleAvatar) return of(undefined);

    const cached = this.cache.get(googleAvatar);
    if (cached) {
      if (Date.now() <= cached.expiry) {
        return of(cached.url);
      } else {
        URL.revokeObjectURL(cached.url);
        this.cache.delete(googleAvatar);
      }
    }

    const url = `https://lh3.googleusercontent.com/a/${googleAvatar}`;
    return this.fetchBlob(url).pipe(
      map(blob => {
        const blobUrl = URL.createObjectURL(blob);
        this.cache.set(googleAvatar, {
          url: blobUrl,
          expiry: Date.now() + this.cacheDuration,
        });
        return blobUrl;
      }),
      catchError(() => of(undefined)),
      shareReplay(1)
    );
  }

  private fetchBlob(url: string): Observable<Blob> {
    return new Observable(observer => {
      fetch(url)
        .then(response => {
          if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
          }
          return response.blob();
        })
        .then(blob => observer.next(blob))
        .catch(error => observer.error(error));
    });
  }
}
