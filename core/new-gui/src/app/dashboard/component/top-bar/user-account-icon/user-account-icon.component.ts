import { Component, OnInit } from '@angular/core';
import { UserAccountService } from '../../../service/user-account/user-account.service';
import { NgbModal, NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { NgbdModalUserAccountLoginComponent } from './user-account-login/ngbdmodal-user-account-login.component';

/**
 * UserAccountIconComponent is used to control user system on the top right corner
 * It includes the button for login/registration/logout
 * It also includes what is shown on the top right
 *
 * @author Adam
 */
@Component({
  selector: 'texera-user-account-icon',
  templateUrl: './user-account-icon.component.html',
  styleUrls: ['./user-account-icon.component.scss']
})
export class UserAccountIconComponent implements OnInit {
  public userName: string = this.getDefaultUserName();

  constructor(
    private modalService: NgbModal,
    private userAccountService: UserAccountService
  ) {
      if (userAccountService.isLogin()) {
        this.userName = this.userAccountService.getUserName();
      }
  }

  ngOnInit() {
    this.detectUserChange();
  }

  /**
   * handle the event when user click on the logout button
   */
  public logOutButton(): void {
    this.userAccountService.logOut();
  }

  /**
   * handle the event when user click on the login (sign in) button
   */
  public loginButton(): void {
    this.openLoginComponent(0);
  }

  /**
   * handle the event when user click on the register (sign up) button
   */
  public registerButton(): void {
    this.openLoginComponent(1);
  }

  /**
   * return true if the user is already login
   */
  public isLogin(): boolean {
    return this.userAccountService.isLogin();
  }

  /**
   * This method will open the login/register pop up
   * It will switch to the tab based on the mode numer given
   * @param mode 0 indicates login and 1 indicates registration
   */
  private openLoginComponent(mode: 0 | 1): void {
    const modalRef: NgbModalRef = this.modalService.open(NgbdModalUserAccountLoginComponent);
    modalRef.componentInstance.selectedTab = mode;
  }

  /**
   * this method will change the user name on screen when receive userChangeEvent
   */
  private detectUserChange(): void {
    this.userAccountService.getUserChangeEvent()
      .subscribe(
        () => {
          if (this.userAccountService.isLogin()) {
            this.userName = this.userAccountService.getUserName();
          } else {
            this.userName = this.getDefaultUserName();
          }
        }
      );

  }

  /**
   * this method will return the default name show on screen
   */
  private getDefaultUserName(): string {
    return 'User';
  }

}
