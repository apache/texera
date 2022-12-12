import { Component } from "@angular/core";
import { UserService } from "../../../../common/service/user/user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-google-login",
  templateUrl: "./google-login.component.html",
  styleUrls: ["./google-login.component.scss"],
})
export class GoogleLoginComponent {
  constructor(private userService: UserService) {}

  /**
   * this method will retrieve a usable Google OAuth Instance first,
   * with that available instance, get googleUsername and authorization code respectively,
   * then sending the code to the backend
   */
  public googleLogin(): void {
    this.userService
      .googleLogin()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.userService.getCurrentUser()) {
          location.href = "dashboard/workflow";
        }
      });
  }
}
