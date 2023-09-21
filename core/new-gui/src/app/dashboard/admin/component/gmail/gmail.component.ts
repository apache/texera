import { Component, OnInit } from "@angular/core";
import { GmailService } from "../../service/gmail.service";
import { FormBuilder, FormGroup, Validators } from "@angular/forms";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
@UntilDestroy()
@Component({
  selector: "texera-gmail",
  templateUrl: "./gmail.component.html",
  styleUrls: ["./gmail.component.scss"],
})
export class GmailComponent implements OnInit {
  public validateForm!: FormGroup;
  isVisible = false;
  public email: String | undefined;
  constructor(private gmailAuthService: GmailService, private formBuilder: FormBuilder) {}

  sendEmail(): void {
    this.gmailAuthService.sendEmail(
      this.validateForm.value.subject,
      this.validateForm.value.content,
      this.validateForm.value.email
    );
    this.isVisible = true;
  }

  ngOnInit(): void {
    this.validateForm = this.formBuilder.group({
      email: [null, [Validators.email, Validators.required]],
      subject: [null, [Validators.required]],
      content: [null, [Validators.required]],
    });
    this.gmailAuthService.auth();
    this.getEmail();
  }

  getEmail() {
    this.gmailAuthService
      .getEmail()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: email => (this.email = email),
        error: (err: unknown) => (this.email = undefined),
      });
  }

  public auth() {
    this.gmailAuthService.client.requestCode();
    this.gmailAuthService.googleCredentialResponse.pipe(untilDestroyed(this)).subscribe(() => this.getEmail());
  }

  public revoke() {
    this.gmailAuthService
      .deleteEmail()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.getEmail());
  }
}
