import { Injectable } from "@angular/core";
import { Observable, Subject } from "rxjs";
import { WorkflowActionService } from "../../../workspace/service/workflow-graph/model/workflow-action.service";

export const DISPLAY_WORKFLOW_VERIONS_EVENT = "display_workflow_versions_event";

@Injectable({
  providedIn: "root",
})
export class WorkflowVersionService {
  private workflowVersionsObservable = new Subject<readonly string[]>();
  constructor(private workflowActionService: WorkflowActionService) {}

  public clickDisplayWorkflowVersions(): void {
    // unhighlight all the current highlighted operators/groups/links
    const elements = this.workflowActionService.getJointGraphWrapper().getCurrentHighlights();
    this.workflowActionService.getJointGraphWrapper().unhighlightElements(elements);

    // emit event for display workflow versions event
    this.workflowVersionsObservable.next([DISPLAY_WORKFLOW_VERIONS_EVENT]);
  }

  public workflowVersionsDisplayObservable(): Observable<readonly string[]> {
    return this.workflowVersionsObservable.asObservable();
  }
}
