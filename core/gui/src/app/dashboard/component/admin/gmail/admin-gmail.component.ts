import { Component, OnInit } from "@angular/core";
import { GmailService } from "../../../../common/service/gmail/gmail.service";
import { FormBuilder, FormGroup, Validators } from "@angular/forms";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
@UntilDestroy()
@Component({
  selector: "texera-gmail",
  templateUrl: "./admin-gmail.component.html",
  styleUrls: ["./admin-gmail.component.scss"],
})
export class AdminGmailComponent implements OnInit {
  public validateForm!: FormGroup;
  public email: String | undefined;
  constructor(
    private gmailAuthService: GmailService,
    private formBuilder: FormBuilder
  ) {}

  ngOnInit(): void {
    this.validateForm = this.formBuilder.group({
      email: [null, [Validators.email, Validators.required]],
      subject: [null, [Validators.required]],
      content: [null, [Validators.required]],
    });
    this.getSenderEmail();
  }

  getSenderEmail() {
    this.gmailAuthService
      .getSenderEmail()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: email => (this.email = email),
        error: (err: unknown) => {
          this.email = undefined;
          console.log(err);
        },
      });
  }
  sendTestEmail(): void {
    this.gmailAuthService.sendEmail(
      this.validateForm.value.subject,
      this.validateForm.value.content,
      this.validateForm.value.email
    );
  }
}
