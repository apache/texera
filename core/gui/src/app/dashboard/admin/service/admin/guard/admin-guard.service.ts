import { Injectable } from "@angular/core";
import { CanActivate, Router } from "@angular/router";
import { UserService } from "../../../../../common/service/user/user.service";

/**
 * AuthGuardService is a service can tell the router whether
 * it should allow navigation to a requested route.
 */
@Injectable()
export class AdminGuardService implements CanActivate {
  constructor(
    private userService: UserService,
    private router: Router
  ) {}

  canActivate(): boolean {
    if (this.userService.isAdmin()) {
      return true;
    } else {
      this.router.navigate(["/dashboard/workflow"]);
      return false;
    }
  }
}
