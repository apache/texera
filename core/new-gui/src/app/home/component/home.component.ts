import { Component } from "@angular/core";
import { environment } from "../../../environments/environment";

@Component({
  selector: "texera-login",
  templateUrl: "./home.component.html",
  styleUrls: ["./home.component.scss"],
})
export class HomeComponent {
  localLogin = environment.localLogin;
}
