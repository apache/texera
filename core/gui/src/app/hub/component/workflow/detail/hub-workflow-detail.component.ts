import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ActivatedRoute } from "@angular/router";
import { HubWorkflowService } from "../../../service/workflow/hub-workflow.service";
import { User } from "../../../../common/type/user";

@UntilDestroy()
@Component({
  selector: "texera-hub-workflow-result",
  templateUrl: "hub-workflow-detail.component.html",
  styleUrls: ["hub-workflow-detail.component.scss"],
})
export class HubWorkflowDetailComponent implements OnInit {
  wid: number;
  ownerUser!: User;
  workflowName: string = "";

  workflow = {
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

  constructor(
    private hubWorkflowService: HubWorkflowService,
    private route: ActivatedRoute
  ) {
    this.wid = this.route.snapshot.params.id;
  }

  ngOnInit() {
    this.hubWorkflowService
      .getOwnerUser(this.wid)
      .pipe(untilDestroyed(this))
      .subscribe(owner => {
        this.ownerUser = owner;
      });
    this.hubWorkflowService
      .getWorkflowName(this.wid)
      .pipe(untilDestroyed(this))
      .subscribe(workflowName => {
        this.workflowName = workflowName;
      });
  }
}
