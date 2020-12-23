import { Injectable } from '@angular/core';
import { Workflow, WorkflowInfo } from '../../../common/type/workflow';
import { mapToRecord } from '../../../common/util/map';
import { localGetObject, localSetObject } from '../../../common/util/storage';
import { Breakpoint, Point } from '../../types/workflow-common.interface';
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
export class CacheWorkflowService {

  private static readonly LOCAL_STORAGE_KEY: string = 'workflow';
  private static readonly DEFAULT_WORKFLOW_NAME: string = 'Untitled Workflow';

  private static readonly DEFAULT_WORKFLOW: Workflow = {
    wid: undefined,
    name: CacheWorkflowService.DEFAULT_WORKFLOW_NAME,
    content: {
      operators: [],
      operatorPositions: {},
      links: [],
      groups: [],
      breakpoints: {}
    },
    creationTime: 0,
    lastModifiedTime: 0
  };

  constructor(
    private workflowActionService: WorkflowActionService,
    private operatorMetadataService: OperatorMetadataService
  ) {
    this.handleAutoCacheWorkFlow();

    this.operatorMetadataService.getOperatorMetadata()
        .filter(metadata => metadata.operators.length !== 0)
        .subscribe(() => this.workflowActionService.reloadWorkflow(this.getCachedWorkflow()));
  }

  /**
   * This method will listen to all the workflow change event happening
   *  on the property panel and the workflow editor paper.
   */
  public handleAutoCacheWorkFlow(): void {
    this.workflowActionService.workflowChanged().debounceTime(100).subscribe(
      () => {
        const workflow1 = this.workflowActionService.getTexeraGraph();

        const operators = workflow1.getAllOperators();
        const links = workflow1.getAllLinks();
        const groups = this.workflowActionService.getOperatorGroup().getAllGroups().map(group => {
          return {
            groupID: group.groupID, operators: mapToRecord(group.operators),
            links: mapToRecord(group.links), inLinks: group.inLinks, outLinks: group.outLinks,
            collapsed: group.collapsed
          };
        });
        const operatorPositions: { [key: string]: Point } = {};
        const breakpointsMap = workflow1.getAllLinkBreakpoints();
        const breakpoints: Record<string, Breakpoint> = {};
        breakpointsMap.forEach((value, key) => (breakpoints[key] = value));
        workflow1.getAllOperators().forEach(op => operatorPositions[op.operatorID] =
          this.workflowActionService.getOperatorGroup().getOperatorPositionByGroup(op.operatorID));

        const cachedWorkflow: WorkflowInfo = {
          operators, operatorPositions, links, groups, breakpoints
        };
        let workflow: Workflow|null = this.getCachedWorkflow();
        if (workflow == null) {
          workflow = CacheWorkflowService.DEFAULT_WORKFLOW;
        }
        workflow.content = cachedWorkflow;
        this.cacheWorkflow(workflow);
      });
  }

  public getCachedWorkflow(): Workflow|null {
    return localGetObject<Workflow>(CacheWorkflowService.LOCAL_STORAGE_KEY);
  }

  public getCachedWorkflowName(): string {
    const workflow = localGetObject<Workflow>(CacheWorkflowService.LOCAL_STORAGE_KEY);
    if (workflow != null) {
      return workflow.name;
    }
    return CacheWorkflowService.DEFAULT_WORKFLOW_NAME;
  }

  getCachedWorkflowID(): number|undefined {
    const workflow = localGetObject<Workflow>(CacheWorkflowService.LOCAL_STORAGE_KEY);
    if (workflow != null) {
      return workflow.wid;
    }
    return undefined;
  }

  public clearCachedWorkflow() {
    localSetObject(CacheWorkflowService.LOCAL_STORAGE_KEY, CacheWorkflowService.DEFAULT_WORKFLOW);
  }

  public cacheWorkflow(workflow: Workflow) {
    localSetObject(CacheWorkflowService.LOCAL_STORAGE_KEY, workflow);
  }

  public setCachedWorkflowId(wid: number|undefined) {
    const workflow = localGetObject<Workflow>(CacheWorkflowService.LOCAL_STORAGE_KEY);
    if (workflow != null) {
      workflow.wid = wid;
      this.cacheWorkflow(workflow);
    }
  }

  public setCachedWorkflowName(name: string) {

    const workflow = localGetObject<Workflow>(CacheWorkflowService.LOCAL_STORAGE_KEY);
    if (workflow != null) {
      workflow.name = name;
      this.cacheWorkflow(workflow);
    }
  }

}
