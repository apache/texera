import {Injectable} from '@angular/core';
import {Observable} from 'rxjs';
import {Breakpoint, OperatorLink, OperatorPredicate, Point} from '../../types/workflow-common.interface';
import {OperatorMetadataService} from '../operator-metadata/operator-metadata.service';
import {WorkflowActionService} from '../workflow-graph/model/workflow-action.service';

/**
 * CachedWorkflow is used to store the information of the workflow
 *  1. all existing operators and their properties
 *  2. operator's position on the JointJS paper
 *  3. operator link predicates
 *
 * When the user refreshes the browser, the CachedWorkflow interface will be
 *  automatically cached and loaded once the refresh completes. This information
 *  will then be used to reload the entire workflow.
 *
 */
export interface CachedWorkflow {
  operators: OperatorPredicate[];
  operatorPositions: { [key: string]: Point | undefined };
  links: OperatorLink[];
  breakpoints: Record<string, Breakpoint>;
}


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
    const cachedWorkflowStr = localStorage.getItem(CacheWorkflowService.LOCAL_STORAGE_KEY);
    if (!cachedWorkflowStr) {
      return;
    }

    const cachedWorkflow: CachedWorkflow = JSON.parse(cachedWorkflowStr);

    const operatorsAndPositions: { op: OperatorPredicate, pos: Point }[] = [];
    cachedWorkflow.operators.forEach(op => {
      const opPosition = cachedWorkflow.operatorPositions[op.operatorID];
      if (!opPosition) {
        throw new Error('position error');
      }
      operatorsAndPositions.push({op: op, pos: opPosition});
    });

    const links: OperatorLink[] = [];
    links.push(...cachedWorkflow.links);

    const breakpoints = new Map(Object.entries(cachedWorkflow.breakpoints));

    this.workflowActionService.addOperatorsAndLinks(operatorsAndPositions, links, breakpoints);

    // operators shouldn't be highlighted during page reload
    const jointGraphWrapper = this.workflowActionService.getJointGraphWrapper();
    jointGraphWrapper.unhighlightOperators(
      jointGraphWrapper.getCurrentHighlightedOperatorIDs());
    //restore the view point
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
      const workflow = this.workflowActionService.getTexeraGraph();

      const operators = workflow.getAllOperators();
      const links = workflow.getAllLinks();
      const operatorPositions: { [key: string]: Point } = {};
      const breakpointsMap = workflow.getAllLinkBreakpoints();
      const breakpoints: Record<string, Breakpoint> = {};
      breakpointsMap.forEach((value, key) => (breakpoints[key] = value));
      workflow.getAllOperators().forEach(op => operatorPositions[op.operatorID] =
        this.workflowActionService.getJointGraphWrapper().getOperatorPosition(op.operatorID));

      const cachedWorkflow: CachedWorkflow = {
        operators, operatorPositions, links, breakpoints
      };

      localStorage.setItem(CacheWorkflowService.LOCAL_STORAGE_KEY, JSON.stringify(cachedWorkflow));
    });
  }

  public getCachedWorkflow(): string | null {
    return localStorage.getItem(CacheWorkflowService.LOCAL_STORAGE_KEY);
  }

}
