import { Injectable } from "@angular/core";
import { Workflow } from "../../../common/type/workflow";
import { localGetObject, localRemoveObject, localSetObject } from "../../../common/util/storage";

@Injectable({
  providedIn: "root",
})
export class WorkflowCacheService {
  private static readonly WORKFLOW_KEY: string = "workflow";

  public getCachedWorkflow(): Workflow | undefined {
    return localGetObject<Workflow>(WorkflowCacheService.WORKFLOW_KEY);
  }

  public resetCachedWorkflow() {
    localRemoveObject(WorkflowCacheService.WORKFLOW_KEY);
  }

  public setCacheWorkflow(workflow: Workflow | undefined): void {
    localSetObject(WorkflowCacheService.WORKFLOW_KEY, workflow);
  }
}
