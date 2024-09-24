import { Component } from "@angular/core";
import { HubWorkflowService } from "../../../service/workflow/hub-workflow.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-hub-workflow-search",
  templateUrl: "hub-workflow-search.component.html",
  styleUrls: ["hub-workflow-search.component.scss"],
})
export class HubWorkflowSearchComponent {
  workflowCount: number | undefined;

  constructor(hubWorkflowService: HubWorkflowService) {
    hubWorkflowService
      .getWorkflowCount()
      .pipe(untilDestroyed(this))
      .subscribe(count => {
        this.workflowCount = count;
      });
  }
}
