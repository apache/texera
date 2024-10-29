import { Component, Input } from "@angular/core";
import { DashboardEntry } from "../../../dashboard/type/dashboard-entry";

@Component({
  selector: "texera-browse-section",
  templateUrl: "./browse-section.component.html",
  styleUrls: ["./browse-section.component.scss"],
})
export class BrowseSectionComponent {
  @Input() workflows: DashboardEntry[] = [];
  @Input() sectionTitle: string = "";
  defaultBackground: string = "../../../../../assets/card_background.jpg";
}
