import { Injectable } from "@angular/core";
import { Router, CanActivate } from "@angular/router";
import { UserService } from "../../../common/service/user/user.service";
@Injectable()
export class AuthGuardService implements CanActivate {
  constructor(private userService: UserService, public router: Router) {}
  canActivate(): boolean {
    if (!this.userService.isLogin()) {
      this.router.navigate(["login"]);
      return false;
    }
    return true;
  }
}
