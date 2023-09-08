import { Component, OnInit } from "@angular/core";
import { GmailService } from "../../service/gmail.service";
@Component({
  selector: "texera-gmail",
  templateUrl: "./gmail.component.html",
  styleUrls: ["./gmail.component.scss"],
})
export class GmailComponent implements OnInit {
  constructor(private gmailAuthService: GmailService) {}
  ngOnInit(): void {
    this.gmailAuthService.auth();
  }

  public auth() {
    this.gmailAuthService.client.requestCode();
  }
}
