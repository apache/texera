import { Component, OnInit } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { UserService } from '../../../../../common/service/user/user.service';


/**
 * NgbdModalUserLoginComponent is the pop up for user login/registration
 *
 * @author Adam
 */
@Component({
  selector: 'texera-ngbdmodal-user-login',
  templateUrl: './ngbdmodal-user-login.component.html',
  styleUrls: ['./ngbdmodal-user-login.component.scss']
})
export class NgbdModalUserLoginComponent implements OnInit {
  public loginUserName: string = '';
  public loginPassword: string = '';
  public registerUserName: string = '';
  public registerPassword: string = '';
  public selectedTab = 0;
  public hide = true;
  public loginErrorMessage: string | undefined;
  public registerErrorMessage: string | undefined;

  constructor(
    public activeModal: NgbActiveModal,
    private userService: UserService) { }

  ngOnInit() {
    this.detectUserChange();
  }

  /**
   * This method is respond for the sign in button in the pop up
   * It will send data inside the text entry to the user service to login
   */
  public login(): void {
    if (this.loginUserName.length === 0 || this.loginPassword.length === 0) {
      return;
    }
    this.loginErrorMessage = undefined;
    this.userService.login(this.loginUserName, this.loginPassword)
      .subscribe(
        res => {
          if (res.code === 0) { // successfully login in
            // TODO show success
            this.activeModal.close();
          } else { // login error
            this.loginErrorMessage = res.message;
          }
        }
      );
  }

  /**
   * This method is respond for the sign on button in the pop up
   * It will send data inside the text entry to the user service to register
   */
  public register(): void {
    if (this.registerUserName.length === 0 || this.registerPassword.length === 0) {
      return;
    }
    this.registerErrorMessage = undefined;

    this.userService.register(this.registerUserName, this.registerPassword)
      .subscribe(
        res => {
          if (res.code === 0) { // successfully register
            // TODO show success
            this.activeModal.close();
          } else { // register error
            this.registerErrorMessage = res.message;
          }
        }
      );
  }

  /**
   * this method will handle the pop up when user successfully login
   */
  private detectUserChange(): void {
    this.userService.getUserChangedEvent()
      .subscribe(
        () => {
          if (this.userService.getUser()) {
            // TODO temporary solution, need improvement
            this.activeModal.close();
          }
        }
      );
  }



}
