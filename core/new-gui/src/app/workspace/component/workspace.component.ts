import { Location } from '@angular/common';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';
import { environment } from '../../../environments/environment';
import { Version } from '../../../environments/version';
import { UserService } from '../../common/service/user/user.service';
import { WorkflowPersistService } from '../../common/service/workflow-persist/workflow-persist.service';
import { Workflow } from '../../common/type/workflow';
import { SchemaPropagationService } from '../service/dynamic-schema/schema-propagation/schema-propagation.service';
import { SourceTablesService } from '../service/dynamic-schema/source-tables/source-tables.service';
import { OperatorMetadataService } from '../service/operator-metadata/operator-metadata.service';
import { ResultPanelToggleService } from '../service/result-panel-toggle/result-panel-toggle.service';
import { UndoRedoService } from '../service/undo-redo/undo-redo.service';
import { WorkflowCacheService } from '../service/workflow-cache/workflow-cache.service';
import { WorkflowActionService } from '../service/workflow-graph/model/workflow-action.service';
import { WorkflowWebsocketService } from '../service/workflow-websocket/workflow-websocket.service';
import { NzMessageService } from 'ng-zorro-antd/message';

@Component({
  selector: 'texera-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss'],
  providers: [
    // uncomment this line for manual testing without opening backend server
    // { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
  ]
})
export class WorkspaceComponent implements OnInit, OnDestroy {

  public gitCommitHash: string = Version.raw;
  public showResultPanel: boolean = false;
  public userSystemEnabled: boolean = environment.userSystemEnabled;

  private subscriptions: Subscription = new Subscription();

  constructor(
    private resultPanelToggleService: ResultPanelToggleService,
    // list additional services in constructor so they are initialized even if no one use them directly
    private sourceTablesService: SourceTablesService,
    private schemaPropagationService: SchemaPropagationService,
    private undoRedoService: UndoRedoService,
    private userService: UserService,
    private workflowCacheService: WorkflowCacheService,
    private workflowPersistService: WorkflowPersistService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService,
    private location: Location,
    private route: ActivatedRoute,
    private operatorMetadataService: OperatorMetadataService,
    private message: NzMessageService
  ) {
    this.subscriptions.add(this.resultPanelToggleService.getToggleChangeStream().subscribe(
      value => this.showResultPanel = value
    ));
  }

  ngOnInit(): void {

    /**
     * On initialization of the workspace, there could be three cases:
     *
     * - with userSystem enabled, usually during prod mode:
     * 1. Accessed by URL `/`, no workflow is in the URL (Cold Start):
     -    - A new `WorkflowActionService.DEFAULT_WORKFLOW` is created, which is an empty workflow with undefined id.
     *    - After an Auto-persist being triggered by a WorkflowAction event, it will create a new workflow in the database
     *    and update the URL with its new ID from database.
     * 2. Accessed by URL `/workflow/:id` (refresh manually, or redirected from dashboard workflow list):
     *    - It will retrieve the workflow from database with the given ID. Because it has an ID, it will be linked to the database
     *    - Auto-persist will be triggered upon all workspace events.
     *
     * - with userSystem disabled, during dev mode:
     * 1. Accessed by URL `/`, with a workflow cached (refresh manually):
     *    - This will trigger the WorkflowCacheService to load the workflow from cache.
     *    - Auto-cache will be triggered upon all workspace events.
     *
     * WorkflowActionService is the single source of the workflow representation. Both WorkflowCacheService and WorkflowPersistService are
     * reflecting changes from WorkflowActionService.
     */


    if (environment.userSystemEnabled) {
      // clear the current workspace, reset as `WorkflowActionService.DEFAULT_WORKFLOW`
      this.workflowActionService.resetAsNewWorkflow();

      // responsible for persisting the workflow to the backend.
      this.registerAutoPersistWorkflow();

      // load workflow with wid if presented in the URL
      if (this.route.snapshot.params.id) {
        const id = this.route.snapshot.params.id;
        // if wid is present in the url, load it from backend
        this.subscriptions.add(this.userService.userChanged().combineLatest(this.operatorMetadataService.getOperatorMetadata()
          .filter(metadata => metadata.operators.length !== 0))
          .subscribe(() => this.loadWorkflowWithID(id)));
      }

    } else {

      // load wid from cache
      const workflow = this.workflowCacheService.getCachedWorkflow();
      const id = workflow?.wid;
      if (id !== undefined) { this.location.go(`/workflow/${id}`); }

      // responsible for saving the existing workflow in cache.
      this.registerAutoCacheWorkFlow();

      // responsible for reloading workflow back to the JointJS paper when the browser refreshes.
      this.registerAutoReloadWorkflowFromCache();

    }

  }

  public ngOnDestroy(): void {
    this.subscriptions.unsubscribe();
  }

  private registerAutoReloadWorkflowFromCache(): void {
    this.subscriptions.add(this.operatorMetadataService.getOperatorMetadata()
      .filter(metadata => metadata.operators.length !== 0)
      .subscribe(() => { this.workflowActionService.reloadWorkflow(this.workflowCacheService.getCachedWorkflow()); }));
  }

  private registerAutoCacheWorkFlow(): void {
    this.subscriptions.add(this.workflowActionService.workflowChanged().debounceTime(100)
      .subscribe(() => {
        this.workflowCacheService.setCacheWorkflow(this.workflowActionService.getWorkflow());
      }));
  }

  private registerAutoPersistWorkflow(): void {
    this.subscriptions.add(this.workflowActionService.workflowChanged().debounceTime(100).subscribe(() => {
      if (this.userService.isLogin()) {
        this.workflowPersistService.persistWorkflow(this.workflowActionService.getWorkflow())
          .subscribe((updatedWorkflow: Workflow) => {
            this.workflowActionService.setWorkflowMetadata(updatedWorkflow);
            this.location.go(`/workflow/${updatedWorkflow.wid}`);
          });
        // to sync up with the updated information, such as workflow.wid
      }
    }));
  }

  private loadWorkflowWithID(id: number): void {
    this.subscriptions.add(this.workflowPersistService.retrieveWorkflow(id).subscribe(
      (workflow: Workflow) => {
        this.workflowActionService.reloadWorkflow(workflow);
        this.undoRedoService.clearUndoStack();
        this.undoRedoService.clearRedoStack();
      },
      () => { this.message.error('You don\'t have access to this workflow, please log in with an appropriate account'); }
    ));
  }
}
