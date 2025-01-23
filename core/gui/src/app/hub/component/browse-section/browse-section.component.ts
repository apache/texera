import { Component, Input } from "@angular/core";
import { DashboardEntry } from "../../../dashboard/type/dashboard-entry";
import { DASHBOARD_HUB_WORKFLOW_RESULT_DETAIL } from "../../../app-routing.constant";

@Component({
  selector: "texera-browse-section",
  templateUrl: "./browse-section.component.html",
  styleUrls: ["./browse-section.component.scss"],
})
export class BrowseSectionComponent {
  @Input() workflows: DashboardEntry[] = [];
  @Input() sectionTitle: string = "";
  defaultBackground: string = "../../../../../assets/card_background.jpg";
  protected readonly DASHBOARD_HUB_WORKFLOW_RESULT_DETAIL = DASHBOARD_HUB_WORKFLOW_RESULT_DETAIL;
}
