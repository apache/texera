import { Injectable } from "@angular/core";
import { Router, CanActivate } from "@angular/router";
import { UserService } from "../user/user.service";
import { environment } from "../../../../environments/environment";
import { AuthService } from "../user/auth.service";

/**
 * AuthGuardService is a service can tell the router whether
 * it should allow navigation to a requested route.
 */
@Injectable()
export class AuthGuardService implements CanActivate {
  constructor(private userService: UserService, private router: Router) {}
  canActivate(): boolean {
    if (this.userService.isLogin() || !environment.userSystemEnabled) {
      return true;
    } else {
      this.router.navigate(["home"]);
      return false;
    }
  }
}
