import { Injectable } from '@angular/core';
import { UserService } from '../../../common/service/user/user.service';
import { User } from '../../../common/type/user';
import { Workflow, WorkflowContent } from '../../../common/type/workflow';
import { localGetObject, localSetObject } from '../../../common/util/storage';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';

/**
 *  CacheWorkflowService is responsible for saving the existing workflow and
 *  reloading back to the JointJS paper when the browser refreshes.
 *
 * It will listens to all the browser action events to update the cached workflow plan.
 * These actions include:
 *  1. operator add
 *  2. operator delete
 *  3. link add
 *  4. link delete
 *  5. operator property change
 *  6. operator position change
 *
 * @author Simon Zhou
 */
@Injectable({
  providedIn: 'root'
})
export class WorkflowCacheService {
  private static readonly LOCAL_STORAGE_KEY: string = 'workflow';
  private static readonly DEFAULT_WORKFLOW_NAME: string = 'Untitled Workflow';

  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowActionService: WorkflowActionService,
    private userService: UserService
  ) {

    this.registerAutoCacheWorkFlow();

    this.userService.userChanged().subscribe((user: User|undefined) => {
      if (user === undefined) {
        this.resetCachedWorkflow();
      }

    });
    this.registerAutoReloadWorkflow();
  }

  /**
   * This method will listen to all the workflow change event happening
   *  on the property panel and the workflow editor paper.
   */
  public registerAutoCacheWorkFlow(): void {
    this.workflowActionService.workflowChanged().debounceTime(100).subscribe(() => {
      this.cacheWorkflow(this.workflowActionService.getWorkflow());

    });
  }

  public getCachedWorkflow(): Workflow|undefined {
    const workflow = localGetObject<Workflow>(WorkflowCacheService.LOCAL_STORAGE_KEY);
    if (workflow === undefined) {
      this.resetCachedWorkflow();
    }
    return workflow;
  }

  public resetCachedWorkflow() {
    this.cacheWorkflow(undefined);
  }

  public cacheWorkflow(workflow: Workflow|undefined): void {
    localSetObject(WorkflowCacheService.LOCAL_STORAGE_KEY, workflow);
  }

  public setCachedWorkflowId(wid: number|undefined) {
    const workflow: Workflow|undefined = this.getCachedWorkflow();
    if (workflow !== undefined) {
      workflow.wid = wid;
    }
    this.cacheWorkflow(workflow);
  }

  public setCachedWorkflowName(name: string) {

    const workflow: Workflow|undefined = this.getCachedWorkflow();
    if (workflow !== undefined) {
      workflow.name = name.length > 0 ? name : WorkflowCacheService.DEFAULT_WORKFLOW_NAME;
    }
    this.cacheWorkflow(workflow);

  }

  public setCacheWorkflowContent(content: WorkflowContent) {
    const workflow: Workflow|undefined = this.getCachedWorkflow();
    if (workflow !== undefined) {
      workflow.content = content;
      this.cacheWorkflow(workflow);
    }
  }

  private registerAutoReloadWorkflow(): void {
    this.operatorMetadataService.getOperatorMetadata()
        .filter(metadata => metadata.operators.length !== 0)
        .subscribe(() => this.workflowActionService.reloadWorkflow(
          this.getCachedWorkflow()));
  }
}
