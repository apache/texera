import { Component } from '@angular/core';
import { NgbModal, NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { UserService } from '../../../../common/service/user/user.service';
import { User } from '../../../../common/type/user';
import { NgbdModalUserLoginComponent } from './user-login/ngbdmodal-user-login.component';

/**
 * UserIconComponent is used to control user system on the top right corner
 * It includes the button for login/registration/logout
 * It also includes what is shown on the top right
 *
 * @author Adam
 */
@Component({
  selector: 'texera-user-icon',
  templateUrl: './user-icon.component.html',
  styleUrls: ['./user-icon.component.scss']
})
export class UserIconComponent {
  public user: User | undefined;

  constructor(
    private modalService: NgbModal,
    private userService: UserService
  ) {

    this.userService.userChanged().subscribe(user => this.user = user);
  }

  /**
   * handle the event when user click on the logout button
   */
  public onClickLogout(): void {
    this.userService.logOut();
  }

  /**
   * handle the event when user click on the login (sign in) button
   */
  public onClickLogin(): void {
    this.openLoginComponent(0);
  }

  /**
   * handle the event when user click on the register (sign up) button
   */
  public onClickRegister(): void {
    this.openLoginComponent(1);
  }

  /**
   * This method will open the login/register pop up
   * It will switch to the tab based on the mode number given
   * @param mode 0 indicates login and 1 indicates registration
   */
  private openLoginComponent(mode: 0 | 1): void {
    const modalRef: NgbModalRef = this.modalService.open(NgbdModalUserLoginComponent);
    modalRef.componentInstance.selectedTab = mode;
  }

}
