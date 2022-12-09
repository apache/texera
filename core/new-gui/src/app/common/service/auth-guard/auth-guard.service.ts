import { Injectable } from "@angular/core";
import { Router, CanActivate } from "@angular/router";
import { UserService } from "../user/user.service";

/**
 * AuthGuardService is a service can tell the router whether
 * it should allow navigation to a requested route.
 */
@Injectable()
export class AuthGuardService implements CanActivate {
  constructor(private userService: UserService, public router: Router) {}
  canActivate(): boolean {
    if (!this.userService.isLogin()) {
      this.router.navigate(["home"]);
      return false;
    }
    return true;
  }
}
