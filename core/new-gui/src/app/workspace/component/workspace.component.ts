import { Location } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { environment } from '../../../environments/environment';
import { Version } from '../../../environments/version';
import { UserService } from '../../common/service/user/user.service';
import { WorkflowPersistService } from '../../common/service/user/workflow-persist/workflow-persist.service';
import { Workflow } from '../../common/type/workflow';
import { DragDropService } from '../service/drag-drop/drag-drop.service';
import { DynamicSchemaService } from '../service/dynamic-schema/dynamic-schema.service';
import { SchemaPropagationService } from '../service/dynamic-schema/schema-propagation/schema-propagation.service';
import { SourceTablesService } from '../service/dynamic-schema/source-tables/source-tables.service';
import { ExecuteWorkflowService } from '../service/execute-workflow/execute-workflow.service';
import { JointUIService } from '../service/joint-ui/joint-ui.service';
import { OperatorMetadataService } from '../service/operator-metadata/operator-metadata.service';
import { ResultPanelToggleService } from '../service/result-panel-toggle/result-panel-toggle.service';
import { UndoRedoService } from '../service/undo-redo/undo-redo.service';
import { ValidationWorkflowService } from '../service/validation/validation-workflow.service';
import { WorkflowCacheService } from '../service/workflow-cache/workflow-cache.service';
import { WorkflowActionService } from '../service/workflow-graph/model/workflow-action.service';
import { WorkflowUtilService } from '../service/workflow-graph/util/workflow-util.service';
import { WorkflowStatusService } from '../service/workflow-status/workflow-status.service';
import { WorkflowWebsocketService } from '../service/workflow-websocket/workflow-websocket.service';

@Component({
  selector: 'texera-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss'],
  providers: [
    // uncomment this line for manual testing without opening backend server
    // { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },

    DragDropService,
    DynamicSchemaService,
    ExecuteWorkflowService,
    JointUIService,
    OperatorMetadataService,
    ResultPanelToggleService,
    SchemaPropagationService,
    SourceTablesService,
    UndoRedoService,
    ValidationWorkflowService,
    WorkflowActionService,
    WorkflowCacheService,
    WorkflowPersistService,
    WorkflowStatusService,
    WorkflowUtilService,
    WorkflowWebsocketService
  ]
})
export class WorkspaceComponent implements OnInit {

  public gitCommitHash: string = Version.raw;
  public showResultPanel: boolean = false;
  public userSystemEnabled: boolean = environment.userSystemEnabled;

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
    private route: ActivatedRoute
  ) {
    this.resultPanelToggleService.getToggleChangeStream().subscribe(
      value => this.showResultPanel = value
    );
  }

  ngOnInit(): void {

    /**
     * On initialization of the workspace, there could be three cases:
     * 1. Accessed by URL `/`, no workflow is cached or in the URL (Cold Start):
     *    - This won't be able to find a workflow on the backend to link with. It starts with the WorkflowCacheService.DEFAULT_WORKFLOW,
     *    which is an empty workflow with undefined id. This workflow will be cached in localStorage upon initialization.
     *    - After an Auto-persist being triggered by a workspace event, it will create a new workflow in the database
     *    and update the cached workflow with its new ID from database.
     * 2. Accessed by URL `/`, with a workflow cached (refresh manually, or create new workflow button):
     *    - This will trigger the WorkflowCacheService to load the workflow from cache. It can be linked to the database if the cached
     *    workflow has ID.
     *    - After an Auto-persist being triggered by a workspace event, if not having an ID, it will create a new workflow in the database
     *    and update the cached workflow with its new ID from database.
     * 3. Accessed by URL `/workflow/:id` (refresh manually, or jump from dashboard workflow list):
     *    - No matter if there exists a cached workflow, it will retrieves the workflow from database with the given ID. the cached workflow
     *    will be overwritten. Because it has an ID, it will be linked to the database
     *    - Auto-persist will be triggered upon all workspace events.
     */
    if (environment.userSystemEnabled) {
      if (this.route.snapshot.params.id) {
        // if wid is present in the url
        this.loadWorkflowWithID(this.route.snapshot.params.id);
      } else {
        // load wid from cache
        const workflow = this.workflowCacheService.getCachedWorkflow();
        const id = workflow?.wid;
        if (id !== undefined) { this.location.go(`/workflow/${id}`); }

      }

    }
  }

  private loadWorkflowWithID(id: number): void {
    this.workflowPersistService.retrieveWorkflow(id).subscribe(
      (workflow: Workflow) => {
        this.workflowActionService.setWorkflow(workflow);
        this.undoRedoService.clearUndoStack();
        this.undoRedoService.clearRedoStack();
        this.workflowCacheService.setCacheWorkflow(this.workflowActionService.getWorkflow());
      },
      () => {
        // TODO: replace with a proper error message with the framework
        alert('You don\'t have access to this workflow, please log in with another account');
        this.workflowCacheService.resetCachedWorkflow();
      }
    );
  }

}
