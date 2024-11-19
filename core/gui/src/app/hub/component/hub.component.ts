import { Component, Input } from "@angular/core";
import { DASHBOARD_ABOUT, DASHBOARD_HOME, DASHBOARD_HUB_WORKFLOW_RESULT } from "../../app-routing.constant";

@Component({
  selector: "texera-hub",
  templateUrl: "hub.component.html",
  styleUrls: ["hub.component.scss"],
})
export class HubComponent {
  @Input() isLogin: boolean = false;
  protected readonly DASHBOARD_HOME = DASHBOARD_HOME;
  protected readonly DASHBOARD_ABOUT = DASHBOARD_ABOUT;
  protected readonly DASHBOARD_HUB_WORKFLOW_RESULT = DASHBOARD_HUB_WORKFLOW_RESULT;
}
