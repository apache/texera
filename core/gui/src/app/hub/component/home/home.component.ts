import { UntilDestroy } from "@ngneat/until-destroy";
import { Component, OnInit } from "@angular/core";
import { environment } from "../../../../environments/environment";

@UntilDestroy()
@Component({
  selector: "texera-login",
  templateUrl: "./home.component.html",
  styleUrls: ["./home.component.scss"],
})
export class HomeComponent {
  localLogin = environment.localLogin;

  constructor() {}
}
