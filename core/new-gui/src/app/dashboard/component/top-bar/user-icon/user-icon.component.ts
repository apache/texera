import { Component } from "@angular/core";
import { UserService } from "../../../../common/service/user/user.service";
import { User } from "../../../../common/type/user";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalService } from "ng-zorro-antd/modal";



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
   * this method will retrieve a usable Google OAuth Instance first,
   * with that available instance, get googleUsername and authorization code respectively,
   * then sending the code to the backend
   */
  public googleLogin(): void {
    this.userService
      .googleLogin()
      .pipe(untilDestroyed(this))
      .subscribe();
  }
}
