import { Component } from "@angular/core";

/**
 * FeatureBarComponent contains buttons for four main sections of the dashboard - Saved Project,
 * User Dictionary, Running Project, and Data Source.
 *
 * Each button links to one route stored in the 'app-routing.module'.
 * By clicking each button, a user can visit different sections in the feature container
 * (the path would change in corresponding to the button).
 *
 * @author Zhaomin Li
 */
@Component({
  selector: "texera-feature-bar",
  templateUrl: "./feature-bar.component.html",
  styleUrls: ["./feature-bar.component.scss"],
})
export class FeatureBarComponent {}
