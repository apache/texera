import { Injectable, Injector } from "@angular/core";
import { HttpErrorResponse, HttpEvent, HttpHandler, HttpInterceptor, HttpRequest } from "@angular/common/http";
import { Observable, throwError } from "rxjs";
import { catchError } from "rxjs/operators";
import { NotificationService } from "./notification/notification.service";
import { UserService } from "./user/user.service";

// error message sent by the backend when the JWT token is invalid
// see DefaultUnauthorizedHandler in dropwizard
export const INVALID_TOKEN_ERROR_MESSAGE = "Credentials are required to access this resource."
@Injectable()
export class GeneralErrorHttpInterceptor implements HttpInterceptor {

  constructor(
    private notificationService: NotificationService,
    private injector: Injector
    ) {}

  public intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    return next.handle(req).pipe(
      catchError((error: HttpErrorResponse) => {
        let errorMessage = '';
        if (error.error instanceof ErrorEvent) {
          // client-side error
          errorMessage = `Error: ${error.error.message}`;
        } else {
          // server-side error
          errorMessage = `Error Code: ${error.status}\nMessage: ${error.message}`;
          if (error.status === 401) {
            // 401 unauthorized with logged in user means 
            // current user token is already expired
            console.log(error)
            const userService = this.injector.get(UserService);
            if (userService.isLogin() && error.error === INVALID_TOKEN_ERROR_MESSAGE) {
              userService.logout();
              this.notificationService.error("You've been logged out due to server restart.");
            }
          } else {
            this.notificationService.error(errorMessage);
          }
        }
        return throwError(() => errorMessage);
      })
    );
  }
}
