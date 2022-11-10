import { Component } from "@angular/core";
import { UserService } from "../../../../common/service/user/user.service";
import { User } from "../../../../common/type/user";
import { UserLoginModalComponent } from "./user-login/user-login-modal.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalService } from "ng-zorro-antd/modal";

/**
 * UserIconComponent is used to control user system on the top right corner
 * It includes the button for login/registration/logout
 * It also includes what is shown on the top right
 */
@UntilDestroy()
@Component({
  selector: "texera-user-icon",
  templateUrl: "./user-icon.component.html",
  styleUrls: ["./user-icon.component.scss"],
})
export class UserIconComponent {
  public user: User | undefined;

  constructor(private modalService: NzModalService, private userService: UserService) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(user => (this.user = user));
  }

  /**
   * handle the event when user click on the logout button
   */
  public onClickLogout(): void {
    this.userService.logout();
  }

  /**
   * handle the event when user click on the login (sign in) button
   */
  public onClickLogin(): void {
    this.openLoginComponent();
  }

  /**
   * This method will open the login/register pop up
   * It will switch to the tab based on the mode number given
   * @param mode 0 indicates login and 1 indicates registration
   */
  private openLoginComponent(): void {
    this.modalService.create({ nzContent: UserLoginModalComponent, nzOkText: null });
  }
}
