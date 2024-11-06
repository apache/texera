import { UntilDestroy } from "@ngneat/until-destroy";
import { Component } from "@angular/core";
import { environment } from "../../../../environments/environment";

@UntilDestroy()
@Component({
  selector: "texera-about",
  templateUrl: "./about.component.html",
  styleUrls: ["./about.component.scss"],
})
export class AboutComponent {
  localLogin = environment.localLogin;

  constructor() {}
}
