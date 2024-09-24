import { Component } from "@angular/core";
import { UntilDestroy } from "@ngneat/until-destroy";
import { ActivatedRoute } from "@angular/router";

@UntilDestroy()
@Component({
  selector: "texera-hub-workflow-result",
  templateUrl: "hub-workflow-detail.component.html",
  styleUrls: ["hub-workflow-detail.component.scss"],
})
export class HubWorkflowDetailComponent {
  wid: string | null;

  workflow = {
    name: "Example Workflow",
    createdBy: "John Doe",
    steps: [
      {
        name: "Step 1: Data Collection",
        description: "Collect necessary data from various sources.",
        status: "Completed",
      },
      {
        name: "Step 2: Data Analysis",
        description: "Analyze the collected data for insights.",
        status: "In Progress",
      },
      {
        name: "Step 3: Report Generation",
        description: "Generate reports based on the analysis.",
        status: "Not Started",
      },
      {
        name: "Step 4: Presentation",
        description: "Present the findings to stakeholders.",
        status: "Not Started",
      },
    ],
  };

  constructor(private route: ActivatedRoute) {
    this.wid = this.route.snapshot.queryParamMap.get("wid");
  }
}
