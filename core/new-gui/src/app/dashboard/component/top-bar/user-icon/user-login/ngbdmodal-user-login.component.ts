import { Component, OnInit } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { UserService } from '../../../../../common/service/user/user.service';
import { User } from '../../../../../common/type/user';
import { isDefined } from '../../../../../common/util/predicate';

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
  public registerUserName: string = '';
  public selectedTab = 0;
  public loginErrorMessage: string | undefined;
  public registerErrorMessage: string | undefined;

  constructor(
    public activeModal: NgbActiveModal,
    private userService: UserService) {
  }

  ngOnInit() {
    this.detectUserChange();
  }

  /**
   * This method is respond for the sign in button in the pop up
   * It will send data inside the text entry to the user service to login
   */
  public login(): void {
    // validate the credentials format
    this.loginErrorMessage = undefined;
    const validation = this.userService.validateUsername(this.loginUserName);
    if (!validation.result) {
      this.loginErrorMessage = validation.message;
      return;
    }

    // validate the credentials with backend
    this.userService.login(this.loginUserName).subscribe(
      () => {
        this.userService.changeUser(<User>{name: this.loginUserName});
        this.activeModal.close();

      }, () => this.loginErrorMessage = 'Incorrect credentials');
  }

  /**
   * This method is respond for the sign on button in the pop up
   * It will send data inside the text entry to the user service to register
   */
  public register(): void {
    // validate the credentials format
    this.registerErrorMessage = undefined;
    const validation = this.userService.validateUsername(this.registerUserName);
    if (!validation.result) {
      this.registerErrorMessage = validation.message;
      return;
    }
    // register the credentials with backend
    this.userService.register(this.registerUserName).subscribe(
      () => {
        this.userService.changeUser(<User>{name: this.registerUserName});
        this.activeModal.close();

      }, () => this.registerErrorMessage = 'Registration failed. Could due to duplicate username.');
  }

  /**
   * this method will handle the pop up when user successfully login
   */
  private detectUserChange(): void {
    // TODO temporary solution, need improvement
    this.userService.userChanged().filter(isDefined).subscribe(() => {
      this.activeModal.close();
    });
  }
}
