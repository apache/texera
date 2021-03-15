import { Location } from '@angular/common';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';
import { environment } from '../../../environments/environment';
import { Version } from '../../../environments/version';
import { UserService } from '../../common/service/user/user.service';
import { WorkflowPersistService } from '../../common/service/user/workflow-persist/workflow-persist.service';
import { Workflow } from '../../common/type/workflow';
import { SchemaPropagationService } from '../service/dynamic-schema/schema-propagation/schema-propagation.service';
import { SourceTablesService } from '../service/dynamic-schema/source-tables/source-tables.service';
import { OperatorMetadataService } from '../service/operator-metadata/operator-metadata.service';
import { ResultPanelToggleService } from '../service/result-panel-toggle/result-panel-toggle.service';
import { UndoRedoService } from '../service/undo-redo/undo-redo.service';
import { WorkflowCacheService } from '../service/workflow-cache/workflow-cache.service';
import { WorkflowActionService } from '../service/workflow-graph/model/workflow-action.service';
import { WorkflowWebsocketService } from '../service/workflow-websocket/workflow-websocket.service';

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
    private operatorMetadataService: OperatorMetadataService
  ) {
    this.subscriptions.add(this.resultPanelToggleService.getToggleChangeStream().subscribe(
      value => this.showResultPanel = value
    ));
  }

  ngOnInit(): void {

    /**
     * On initialization of the workspace, there could be four cases:
     * 1. Accessed by URL `/`, no workflow is cached or in the URL (Cold Start):
     *    - This won't be able to find a workflow on the backend to link with. It starts with the WorkflowActionService.DEFAULT_WORKFLOW,
     *    which is an empty workflow with undefined id. This new empty workflow will be cached in localStorage upon initialization.
     *    - After an Auto-persist being triggered by a workspace event, it will create a new workflow in the database
     *    and update the cached workflow with its new ID from database.
     * 2. Accessed by URL `/`, with a workflow cached (refresh manually, or create new workflow button from workspace):
     *    - This will trigger the WorkflowCacheService to load the workflow from cache. It can be linked to the database if the cached
     *    workflow has ID.
     *    - After an Auto-persist being triggered by a workspace event, if not having an ID, it will create a new workflow in the database
     *    and update the cached workflow with its new ID from database.
     * 3. Accessed by URL `/workflow/:id` (refresh manually, or redirected from dashboard workflow list):
     *    - No matter if there exists a cached workflow, it will retrieves the workflow from database with the given ID. the cached workflow
     *    will be overwritten. Because it has an ID, it will be linked to the database
     *    - Auto-persist will be triggered upon all workspace events.
     * 4. Accessed by URL `/workflow/new` (create new workflow button from dashboard):
     *    - A new empty workflow is created.
     *    - After an Auto-persist being triggered by a workspace event, if not having an ID, it will create a new workflow in the database
     *    and update the cached workflow with its new ID from database.
     *
     * WorkflowActionService is the single source of the workflow representation. Both WorkflowCacheService and WorkflowPersistService are
     * reflecting changes from WorkflowActionService.
     */

    // responsible for saving the existing workflow.
    this.registerAutoCacheWorkFlow();

    // responsible for reloading workflow back to the JointJS paper when the browser refreshes.
    this.registerAutoReloadWorkflow();

    if (environment.userSystemEnabled) {
      if (this.route.snapshot.params.id) {
        const id = this.route.snapshot.params.id;
        if (id === 'new') {
          // if new is present in the url, create a new workflow
          this.workflowCacheService.resetCachedWorkflow();
          this.workflowActionService.resetAsNewWorkflow();
          this.location.go('/');
        } else {
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
      }

      this.registerAutoPersistWorkflow();
    }

  }

  public ngOnDestroy(): void {
    this.subscriptions.unsubscribe();
  }

  private registerAutoReloadWorkflow(): void {
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
            this.workflowCacheService.setCacheWorkflow(this.workflowActionService.getWorkflow());
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
        this.workflowCacheService.setCacheWorkflow(this.workflowActionService.getWorkflow());
      },
      () => { this.noAccessToWorkflow(); }
    ));
  }

  private noAccessToWorkflow(): void {
    this.undoRedoService.clearUndoStack();
    this.undoRedoService.clearRedoStack();
    this.workflowCacheService.resetCachedWorkflow();
    this.workflowActionService.reloadWorkflow(this.workflowCacheService.getCachedWorkflow());
    this.location.go(`/`);
    // TODO: replace with a proper error message with the framework
    alert('You don\'t have access to this workflow, please log in with another account');
  }
}
