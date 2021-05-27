import { Component, OnInit } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { UserService } from '../../../../../common/service/user/user.service';
import { User } from '../../../../../common/type/user';
import { Validators, FormControl, FormGroup, FormBuilder} from '@angular/forms';
import { isDefined } from '../../../../../common/util/predicate';
import { environment } from '../../../../../../environments/environment'
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
  public selectedTab = 0;
  public loginErrorMessage: string | undefined;
  public registerErrorMessage: string | undefined;
  public allForms: FormGroup;
  public oauthInstance!: gapi.auth2.GoogleAuth;
  public user!: gapi.auth2.GoogleUser;
  public authCode!: string ;
  public gapiSetUp: boolean = false;
  public userName!: string | undefined;
  public error!: string;

  constructor(
    private formBuilder: FormBuilder,
    public activeModal: NgbActiveModal,
    private userService: UserService) {
      this.allForms = this.formBuilder.group({
        loginUserName: new FormControl('', [Validators.required]),
        registerUserName: new FormControl('', [Validators.required]),
        loginPassword: new FormControl('', [Validators.required]),
        registerPassword : new FormControl('', [Validators.required]),
        registerConfirmationPassword : new FormControl('', [Validators.required]),
      });
  }

  ngOnInit() {
    this.detectUserChange();
  }

  public errorMessageUsernameNull(): string{
    return "Username required";
  }

  public errorMessagePasswordNull(): string{
    return this.allForms.controls["registerPassword"].hasError('required') ? "Password required"
          : this.allForms.controls["registerConfirmationPassword"].hasError('required') ? "Confirmation required"
          : this.allForms.controls["loginPassword"].hasError('required') ? "Password required"
          : "";
  }

  /**
   * This method is respond for the sign in button in the pop up
   * It will send data inside the text entry to the user service to login
   */
  public login(): void {
    // validate the credentials format
    this.loginErrorMessage = undefined;
    const validation = this.userService.validateUsername(this.allForms.get("loginUserName")!.value);
    if (!validation.result) {
      this.loginErrorMessage = validation.message;
      return;
    }

    // validate the credentials with backend
    this.userService.login(this.allForms.get("loginUserName")!.value.trim(), this.allForms.get("loginPassword")!.value).subscribe(
      () => {
        this.userService.changeUser(<User>{name: this.allForms.get("loginUserName")!.value});
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
    const validation = this.userService.validateUsername(this.allForms.get("registerUserName")!.value.trim());
    if (this.allForms.get("registerPassword")!.value.length < 6){
      this.registerErrorMessage = 'Password length should be greater than 5';
      return;
    }
    if (this.allForms.get("registerPassword")!.value !== this.allForms.get("registerConfirmationPassword")!.value){
      this.registerErrorMessage = 'Passwords do not match';
      return;
    }
    if (!validation.result) {
      this.registerErrorMessage = validation.message;
      return;
    }
    // register the credentials with backend
    this.userService.register(this.allForms.get("registerUserName")!.value.trim(), this.allForms.get("registerPassword")!.value).subscribe(
      () => {
        this.userService.changeUser(<User>{name: this.allForms.get("registerUserName")!.value.trim()});
        this.activeModal.close();

      }, () => this.registerErrorMessage = 'Registration failed. Could due to duplicate username.');
  }

  /**
   * this method will init A Google Oauth instance and gapi
   */
  public async initGoogleOauth(): Promise<void> {
    // load gapi
    const gapiLoad = new Promise((resolve) => {
      gapi.load('auth2', resolve);
    });
    // call gapi.auth2 init
    return gapiLoad.then(async () => {
      await gapi.auth2
        .init({ client_id: environment.google.clientID })
        .then(auth => {
          this.oauthInstance = auth;
          this.gapiSetUp = true;
        });
    });
  }

  /**
   * this method will return the authorization code
   */
  public async authenticate(): Promise<void> {
    // initialize gapi if not done yet
    if (!this.gapiSetUp) {
      await this.initGoogleOauth();
    }
    await this.oauthInstance.signIn().then(
      user => this.user = user,
      error => this.error = error
    )
    await this.oauthInstance.grantOfflineAccess().then(
      code => this.authCode = code['code']
    )

    // set the user name
    this.userName = this.user?.getBasicProfile().getName()

    this.userService.googleLogin(this.authCode).subscribe(
      () => {
        this.userService.changeUser(<User>{name: this.userName});
        this.activeModal.close();
      }, () => this.loginErrorMessage = 'Incorrect credentials');
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

