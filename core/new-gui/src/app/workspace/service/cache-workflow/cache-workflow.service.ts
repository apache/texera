import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { Breakpoint, OperatorLink, OperatorPredicate, Point } from '../../types/workflow-common.interface';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { WorkflowInfo, Workflow } from '../../../common/type/workflow';
import { localGetObject, localSetObject } from '../../../common/util/storage';


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
      breakpoints: {},
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
      .subscribe(() => this.loadWorkflow());
  }

  /**
   * When the browser reloads, this method will be called to reload
   *  previously created workflow stored in the local storage onto
   *  the JointJS paper.
   */
  public loadWorkflow(): void {
    // remove the existing operators on the paper currently
    this.workflowActionService.deleteOperatorsAndLinks(
      this.workflowActionService.getTexeraGraph().getAllOperators().map(op => op.operatorID), []);

    // get items in the storage
    const workflow = localGetObject<Workflow>(CacheWorkflowService.LOCAL_STORAGE_KEY);
    if (workflow == null) {
      return;
    }

    const workflowInfo: WorkflowInfo = workflow.content;

    const operatorsAndPositions: { op: OperatorPredicate, pos: Point }[] = [];
    workflowInfo.operators.forEach(op => {
      const opPosition = workflowInfo.operatorPositions[op.operatorID];
      if (!opPosition) {
        throw new Error('position error');
      }
      operatorsAndPositions.push({op: op, pos: opPosition});
    });

    const links: OperatorLink[] = [];
    links.push(...workflowInfo.links);

    const breakpoints = new Map(Object.entries(workflowInfo.breakpoints));

    this.workflowActionService.addOperatorsAndLinks(operatorsAndPositions, links, breakpoints);

    // operators shouldn't be highlighted during page reload
    const jointGraphWrapper = this.workflowActionService.getJointGraphWrapper();
    jointGraphWrapper.unhighlightOperators(
      jointGraphWrapper.getCurrentHighlightedOperatorIDs());
    // restore the view point
    this.workflowActionService.getJointGraphWrapper().restoreDefaultZoomAndOffset();
  }

  /**
   * This method will listen to all the workflow change event happening
   *  on the property panel and the workflow editor paper.
   */
  public handleAutoCacheWorkFlow(): void {
    Observable.merge(
      this.workflowActionService.getTexeraGraph().getOperatorAddStream(),
      this.workflowActionService.getTexeraGraph().getOperatorDeleteStream(),
      this.workflowActionService.getTexeraGraph().getLinkAddStream(),
      this.workflowActionService.getTexeraGraph().getLinkDeleteStream(),
      this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream(),
      this.workflowActionService.getTexeraGraph().getBreakpointChangeStream(),
      this.workflowActionService.getJointGraphWrapper().getOperatorPositionChangeEvent()
    ).debounceTime(100).subscribe(() => {
      const workflow1 = this.workflowActionService.getTexeraGraph();

      const operators = workflow1.getAllOperators();
      const links = workflow1.getAllLinks();
      const operatorPositions: { [key: string]: Point } = {};
      const breakpointsMap = workflow1.getAllLinkBreakpoints();
      const breakpoints: Record<string, Breakpoint> = {};
      breakpointsMap.forEach((value, key) => (breakpoints[key] = value));
      workflow1.getAllOperators().forEach(op => operatorPositions[op.operatorID] =
        this.workflowActionService.getJointGraphWrapper().getOperatorPosition(op.operatorID));

      const cachedWorkflow: WorkflowInfo = {
        operators, operatorPositions, links, breakpoints
      };
      let workflow: Workflow | null = this.getCachedWorkflow();
      if (workflow == null) {
        workflow = CacheWorkflowService.DEFAULT_WORKFLOW;
      }
      workflow.content = cachedWorkflow;
      this.cacheWorkflow(workflow);
    });
  }

  public getCachedWorkflow(): Workflow | null {
    return localGetObject<Workflow>(CacheWorkflowService.LOCAL_STORAGE_KEY);
  }

  public getCachedWorkflowName(): string {
    const workflow = localGetObject<Workflow>(CacheWorkflowService.LOCAL_STORAGE_KEY);
    if (workflow != null) {
      return workflow.name;
    }
    return CacheWorkflowService.DEFAULT_WORKFLOW_NAME;
  }

  getCachedWorkflowID(): number | undefined {
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

  public setCachedWorkflowId(wid: number | undefined) {
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
