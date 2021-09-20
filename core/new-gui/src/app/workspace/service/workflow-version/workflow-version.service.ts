import { Injectable } from "@angular/core";
import { WorkflowVersionEntry } from "../../../dashboard/type/workflow-version-entry";
import { Observable, Subject } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";

@Injectable({
  providedIn: "root",
})
export class WorkflowVersionService {

  private workflowVersions: WorkflowVersionEntry[] = [];
  private workflowVersionsObservable = new Subject<WorkflowVersionEntry[]> ();

  constructor (private workflowActionService: WorkflowActionService) {
  }
  public prepareWorkflowVersions(versionsList: WorkflowVersionEntry[]): void {
    const elements = this.workflowActionService.getJointGraphWrapper().getCurrentHighlights();
    this.workflowActionService.getJointGraphWrapper().unhighlightElements(elements)
    this.workflowVersions = versionsList;
    this.workflowVersionsObservable.next(versionsList);
  }

  public getWorkflowVersions(): WorkflowVersionEntry[] {
    return this.workflowVersions;
  }

  public resetResults(): void {
    this.workflowVersions = [];
  }

  public workflowVersionsChosen() : Observable<WorkflowVersionEntry[]> {
    return this.workflowVersionsObservable.asObservable();
  }
}
