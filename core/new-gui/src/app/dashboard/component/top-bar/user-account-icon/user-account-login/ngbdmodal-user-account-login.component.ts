import { Component, OnInit } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { UserAccountService } from '../../../../service/user-account/user-account.service';


/**
 * NgbdModalUserAccountLoginComponent is the pop up for user login/registration
 *
 * @author Adam
 */
@Component({
  selector: 'texera-ngbdmodal-user-account-login',
  templateUrl: './ngbdmodal-user-account-login.component.html',
  styleUrls: ['./ngbdmodal-user-account-login.component.scss']
})
export class NgbdModalUserAccountLoginComponent implements OnInit {
  public loginUserName: string = '';
  public registerUserName: string = '';
  public selectedTab = 0;


  constructor(
    public activeModal: NgbActiveModal,
    private userAccountService: UserAccountService) { }

  ngOnInit() {
    this.detectUserChange();
  }

  /**
   * This method is respond for the sign in button in the pop up
   * It will send data inside the text entry to the user service to login
   */
  public login(): void {
    if (this.loginUserName.length === 0) {
      return;
    }

    this.userAccountService.loginUser(this.loginUserName)
      .subscribe(
        res => {
          if (res.code === 0) { // successfully login in
            // TODO show success
            this.activeModal.close();
          } else { // login error
            // TODO show error
            console.log(res.message);
          }
        }
      );
  }

  /**
   * This method is respond for the sign on button in the pop up
   * It will send data inside the text entry to the user service to register
   */
  public register(): void {
    if (this.registerUserName.length === 0) {
      return;
    }

    this.userAccountService.registerUser(this.registerUserName)
      .subscribe(
        res => {
          if (res.code === 0) { // successfully register
            // TODO show success
            this.activeModal.close();
          } else { // register error
            // TODO show error
            console.log(res.message);
          }
        }
      );
  }

  /**
   * this method will handle the pop up when user successfully login
   */
  private detectUserChange(): void {
    this.userAccountService.getUserChangeEvent()
      .subscribe(
        () => {
          if (this.userAccountService.isLogin()) {
            // TODO temporary solution, need improvement
            this.activeModal.close();
          }
        }
      );
  }



}
