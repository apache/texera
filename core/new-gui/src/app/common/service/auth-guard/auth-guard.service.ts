import { Injectable } from "@angular/core";
import { Router, CanActivate } from "@angular/router";
import { UserService } from "../user/user.service";
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
