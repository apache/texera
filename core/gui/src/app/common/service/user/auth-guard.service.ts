import { Injectable } from "@angular/core";
import { Router, CanActivate, RouterStateSnapshot, ActivatedRouteSnapshot } from "@angular/router";
import { UserService } from "./user.service";
import { environment } from "../../../../environments/environment";
import { DASHBOARD_HOME } from "../../../app-routing.constant";

/**
 * AuthGuardService is a service can tell the router whether
 * it should allow navigation to a requested route.
 */
@Injectable()
export class AuthGuardService implements CanActivate {
  constructor(
    private userService: UserService,
    private router: Router
  ) {}
  canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean {
    if (this.userService.isLogin() || !environment.userSystemEnabled) {
      return true;
    } else {
      this.router.navigate([DASHBOARD_HOME], { queryParams: { returnUrl: state.url === "/" ? null : state.url } });
      return false;
    }
  }
}
