import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Version } from 'src/environments/version';
import { WorkflowPersistService } from '../../common/service/user/workflow-persist/workflow-persist.service';
import { Workflow } from '../../common/type/workflow';
import { CacheWorkflowService } from '../service/cache-workflow/cache-workflow.service';
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
    OperatorMetadataService,
    DynamicSchemaService,
    SourceTablesService,
    SchemaPropagationService,
    JointUIService,
    WorkflowActionService,
    WorkflowUtilService,
    DragDropService,
    ExecuteWorkflowService,
    UndoRedoService,
    ResultPanelToggleService,
    CacheWorkflowService,
    ValidationWorkflowService,
    WorkflowStatusService,
    WorkflowWebsocketService
  ]
})
export class WorkspaceComponent implements OnInit {

  public gitCommitHash: string = Version.raw;
  public showResultPanel: boolean = false;
  public currentWorkflowName: string = '';

  constructor(
    private resultPanelToggleService: ResultPanelToggleService,
    // list additional services in constructor so they are initialized even if no one use them directly
    private sourceTablesService: SourceTablesService,
    private schemaPropagationService: SchemaPropagationService,
    private cacheWorkflowService: CacheWorkflowService,
    private workflowPersistService: WorkflowPersistService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private route: ActivatedRoute
  ) {

    this.resultPanelToggleService.getToggleChangeStream().subscribe(
      value => this.showResultPanel = value
    );
  }

  ngOnInit(): void {
    // check if workflow id is present in the url
    if (this.route.snapshot.params.id) {
      this.workflowPersistService.retrieveWorkflow(this.route.snapshot.params.id).subscribe(
        (workflow: Workflow) => {
          this.cacheWorkflowService.cacheWorkflow(workflow);
          this.currentWorkflowName = workflow.name;
          this.cacheWorkflowService.loadWorkflow();
        },
        error => {
          alert('You don\'t have access to this workflow, please log in with another account');
        }
      );
    }
    this.currentWorkflowName = this.cacheWorkflowService.getCachedWorkflowName();
  }

}
