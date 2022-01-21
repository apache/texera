import { Component } from "@angular/core";

/**
 * TopBarComponent is the on the top of DashboardComponent
 * the top bar contains Texera icon. It leads the user to
 * the Texera workspace by clicking the Icon
 *
 * //for future proposal//
 * The top bar contains the user profile and a drop-down menu
 * which allows users to sign in and sign out. Currently, it is
 * not functioning due to the lack of API. Later, if the API is completed, we can implement
 * the verification and add the client token (/:client_token/Dashboard/..)
 * to the path to make certain HTTP requests and fetch user data.
 *
 * @author Zhaomin Li
 */
@Component({
  selector: "texera-top-bar",
  templateUrl: "./top-bar.component.html",
  styleUrls: ["./top-bar.component.scss"],
})
export class TopBarComponent {}
