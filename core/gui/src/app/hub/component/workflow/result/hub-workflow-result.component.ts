import { Component } from "@angular/core";
import { HubWorkflowService } from "../../../service/workflow/hub-workflow.service";
import { HubWorkflow } from "../../type/hub-workflow.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-hub-workflow-result",
  templateUrl: "hub-workflow-result.component.html",
  styleUrls: ["hub-workflow-result.component.scss"],
})
export class HubWorkflowResultComponent {
  listOfWorkflows: HubWorkflow[] = [];

  constructor(hubWorkflowService: HubWorkflowService) {
    hubWorkflowService
      .getWorkflowList()
      .pipe(untilDestroyed(this))
      .subscribe(workflows => {
        this.listOfWorkflows = workflows;
      });
  }
}
