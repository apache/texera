import { Component, OnInit } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { UserService } from '../../../../../common/service/user/user.service';
import { User } from '../../../../../common/type/user';
import {Validators, FormControl, FormGroup, FormBuilder, ReactiveFormsModule} from '@angular/forms';
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
  public loginPassword = new FormControl('', [Validators.required]);
  public registerUserName: string = '';
  public registerPassword = new FormControl('', [Validators.required]);
  public registerConfirmationPassword = new FormControl('', [Validators.required]);
  public selectedTab = 0;
  public loginErrorMessage: string | undefined;
  public registerErrorMessage: string | undefined;

  constructor(
    private fb: FormBuilder,
    public activeModal: NgbActiveModal,
    private userService: UserService) {
  }

  ngOnInit() {
    this.detectUserChange();
  }

  signupForms = new FormGroup(
    {
      password: new FormControl('', [Validators.required]),
      verifyPassword: new FormControl('', [Validators.required]),
    },
  )

  public errorMessageLoginPasswordNull(): string{
    return this.loginPassword.hasError('required') ? "Password required" : "";
  }

  public errorMessageRegisterPasswordNull(): string{
    return this.registerPassword.hasError('required') ? "Password required" : "";
  }

  public errorMessageRegisterConfirmationPasswordNull(): string{
    return this.registerConfirmationPassword.hasError('required') ? "Confirmation required" : "";
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
    this.userService.login(this.loginUserName, this.loginPassword.value).subscribe(
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
    const validation = this.userService.validateUsername(this.registerUserName.trim());
    if (this.registerPassword.value.length < 6){
      this.registerErrorMessage = 'Password length should be greater than 5';
      return; 
    }
    if (this.registerPassword.value !== this.registerConfirmationPassword.value){
      this.registerErrorMessage = 'Passwords do not match';
      return; 
    }
    if (!validation.result) {
      this.registerErrorMessage = validation.message;
      return;
    }
    // register the credentials with backend
    this.userService.register(this.registerUserName.trim(), this.registerPassword.value).subscribe(
      () => {
        this.userService.changeUser(<User>{name: this.registerUserName.trim()});
        this.activeModal.close();

      }, () => this.registerErrorMessage = 'Registration failed. Could due to duplicate username.');
  }

  /**
   * this method will handle the pop up when user successfully login
   */
  private detectUserChange(): void {
    this.userService.userChanged().subscribe(
      () => {
        if (this.userService.getUser()) {
          // TODO temporary solution, need improvement
          this.activeModal.close();
        }
      }
    );
  }

}

