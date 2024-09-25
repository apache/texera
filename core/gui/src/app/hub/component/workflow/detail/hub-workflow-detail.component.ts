import {AfterViewInit, Component, OnInit, HostListener, OnDestroy, ViewChild, ViewContainerRef} from "@angular/core";
import {UntilDestroy, untilDestroyed} from "@ngneat/until-destroy";
import {ActivatedRoute, Router} from "@angular/router";
import {Version} from "../../../../../environments/version";
import {environment} from "../../../../../environments/environment";
import {UserService} from "../../../../common/service/user/user.service";
import {SchemaPropagationService} from "../../../../workspace/service/dynamic-schema/schema-propagation/schema-propagation.service";
import {OperatorReuseCacheStatusService} from "../../../../workspace/service/workflow-status/operator-reuse-cache-status.service";
import {WorkflowConsoleService} from "../../../../workspace/service/workflow-console/workflow-console.service";
import {UndoRedoService} from "../../../../workspace/service/undo-redo/undo-redo.service";
import {WorkflowCacheService} from "../../../../workspace/service/workflow-cache/workflow-cache.service";
import {WorkflowPersistService} from "../../../../common/service/workflow-persist/workflow-persist.service";
import {WorkflowWebsocketService} from "../../../../workspace/service/workflow-websocket/workflow-websocket.service";
import {WorkflowActionService} from "../../../../workspace/service/workflow-graph/model/workflow-action.service";
import {Location} from "@angular/common";
import {OperatorMetadataService} from "../../../../workspace/service/operator-metadata/operator-metadata.service";
import {NzMessageService} from "ng-zorro-antd/message";
import {NotificationService} from "../../../../common/service/notification/notification.service";
import {CodeEditorService} from "../../../../workspace/service/code-editor/code-editor.service";
import {debounceTime, distinctUntilChanged, filter, switchMap} from "rxjs/operators";
import {Workflow} from "../../../../common/type/workflow";
import {of} from "rxjs";
import {isDefined} from "../../../../common/util/predicate";
import { HubWorkflowService } from "../../../service/workflow/hub-workflow.service";
import { User } from "src/app/common/type/user";
import { NzModalService } from "ng-zorro-antd/modal";
import { LocalLoginComponent } from "../../home/local-login/local-login.component";

export const SAVE_DEBOUNCE_TIME_IN_MS = 5000;

@UntilDestroy()
@Component({
  selector: "texera-hub-workflow-result",
  templateUrl: "hub-workflow-detail.component.html",
  styleUrls: ["hub-workflow-detail.component.scss"],
})
export class HubWorkflowDetailComponent implements AfterViewInit, OnDestroy {
  wid: number;
  clonedWorklowId: number | undefined;
  workflowName: string = "";
  ownerUser!: User;
  private currentUser?: User;
  public clonePrompt: String = "";
  workflow = {
    steps: [
      {
        name: "Step 1: Data Collection",
        description: "Collect necessary data from various sources.",
        status: "Completed",
      },
      {
        name: "Step 2: Data Analysis",
        description: "Analyze the collected data for insights.",
        status: "In Progress",
      },
      {
        name: "Step 3: Report Generation",
        description: "Generate reports based on the analysis.",
        status: "Not Started",
      },
      {
        name: "Step 4: Presentation",
        description: "Present the findings to stakeholders.",
        status: "Not Started",
      },
    ],
  };

  public pid?: number = undefined;
  public gitCommitHash: string = Version.raw;
  public showResultPanel: boolean = false;
  userSystemEnabled = environment.userSystemEnabled;
  @ViewChild("codeEditor", { read: ViewContainerRef }) codeEditorViewRef!: ViewContainerRef;
  constructor(
    private userService: UserService,
    // list additional services in constructor so they are initialized even if no one use them directly
    private schemaPropagationService: SchemaPropagationService,
    private operatorReuseCacheStatus: OperatorReuseCacheStatusService,
    private workflowConsoleService: WorkflowConsoleService,
    private undoRedoService: UndoRedoService,
    private workflowCacheService: WorkflowCacheService,
    private workflowPersistService: WorkflowPersistService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService,
    private location: Location,
    private route: ActivatedRoute,
    private operatorMetadataService: OperatorMetadataService,
    private message: NzMessageService,
    private router: Router,
    private notificationService: NotificationService,
    private codeEditorService: CodeEditorService,
    private hubWorkflowService: HubWorkflowService,
    private modalService: NzModalService,
  ) {
    this.wid = this.route.snapshot.params.id;
  }

  ngOnInit() {
    this.hubWorkflowService
      .getOwnerUser(this.wid)
      .pipe(untilDestroyed(this))
      .subscribe(owner => {
        this.ownerUser = owner;
      });
    this.hubWorkflowService
      .getWorkflowName(this.wid)
      .pipe(untilDestroyed(this))
      .subscribe(workflowName => {
        this.workflowName = workflowName;
      });
  }

  ngAfterViewInit(): void {
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
    // clear the current workspace, reset as `WorkflowActionService.DEFAULT_WORKFLOW`
    this.workflowActionService.resetAsNewWorkflow();

    if (this.userSystemEnabled) {
      this.registerReEstablishWebsocketUponWIdChange();
    } else {
      let wid = this.route.snapshot.params.id ?? 0;
      this.workflowWebsocketService.openWebsocket(wid);
    }

    this.registerLoadOperatorMetadata();

    this.codeEditorService.vc = this.codeEditorViewRef;
  }

  @HostListener("window:beforeunload")
  ngOnDestroy() {
    if (this.workflowPersistService.isWorkflowPersistEnabled()) {
      const workflow = this.workflowActionService.getWorkflow();
      this.workflowPersistService.persistWorkflow(workflow).pipe(untilDestroyed(this)).subscribe();
    }

    console.log("before clear")
    this.codeEditorViewRef.clear();
    this.workflowWebsocketService.closeWebsocket();
    this.workflowActionService.clearWorkflow();
    console.log("end of on destroy")
  }

  registerAutoCacheWorkFlow(): void {
    this.workflowActionService
      .workflowChanged()
      .pipe(debounceTime(SAVE_DEBOUNCE_TIME_IN_MS))
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.workflowCacheService.setCacheWorkflow(this.workflowActionService.getWorkflow());
      });
  }

  registerAutoPersistWorkflow(): void {
    this.workflowActionService
      .workflowChanged()
      .pipe(debounceTime(SAVE_DEBOUNCE_TIME_IN_MS))
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.userService.isLogin() && this.workflowPersistService.isWorkflowPersistEnabled()) {
          this.workflowPersistService
            .persistWorkflow(this.workflowActionService.getWorkflow())
            .pipe(untilDestroyed(this))
            .subscribe((updatedWorkflow: Workflow) => {
              if (this.workflowActionService.getWorkflowMetadata().wid !== updatedWorkflow.wid) {
                this.location.go(`/workflow/${updatedWorkflow.wid}`);
              }
              this.workflowActionService.setWorkflowMetadata(updatedWorkflow);
            });
          // to sync up with the updated information, such as workflow.wid
        }
      });
  }

  loadWorkflowWithId(wid: number): void {
    // disable the workspace until the workflow is fetched from the backend
    this.workflowActionService.disableWorkflowModification();
    this.workflowPersistService
      .retrievePublicWorkflow(wid)
      .pipe(untilDestroyed(this))
      .subscribe(
        (workflow: Workflow) => {
          this.workflowActionService.setNewSharedModel(wid, this.userService.getCurrentUser());
          // remember URL fragment
          const fragment = this.route.snapshot.fragment;
          // load the fetched workflow
          this.workflowActionService.reloadWorkflow(workflow);
          this.workflowActionService.enableWorkflowModification();
          // set the URL fragment to previous value
          // because reloadWorkflow will highlight/unhighlight all elements
          // which will change the URL fragment
          this.router.navigate([], {
            relativeTo: this.route,
            fragment: fragment !== null ? fragment : undefined,
            preserveFragment: false,
          });
          // highlight the operator, comment box, or link in the URL fragment
          if (fragment) {
            if (this.workflowActionService.getTexeraGraph().hasElementWithID(fragment)) {
              this.workflowActionService.highlightElements(false, fragment);
            } else {
              this.notificationService.error(`Element ${fragment} doesn't exist`);
              // remove the fragment from the URL
              this.router.navigate([], { relativeTo: this.route });
            }
          }
          // clear stack
          this.undoRedoService.clearUndoStack();
          this.undoRedoService.clearRedoStack();
        },
        () => {
          console.log("in error")
          this.workflowActionService.resetAsNewWorkflow();
          // enable workspace for modification
          this.workflowActionService.enableWorkflowModification();
          // clear stack
          this.undoRedoService.clearUndoStack();
          this.undoRedoService.clearRedoStack();
          this.message.error("You don't have access to this workflow, please log in with an appropriate account");
        }
      );
  }

  registerLoadOperatorMetadata() {
    this.operatorMetadataService
      .getOperatorMetadata()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        console.log(this.wid)
        let wid = this.route.snapshot.params.id;
        console.log("this is what got calculated")
        console.log(wid)
        wid = this.wid;
        if (environment.userSystemEnabled) {
          // load workflow with wid if presented in the URL
          if (wid) {
            // if wid is present in the url, load it from the backend
            this.userService
              .userChanged()
              .pipe(untilDestroyed(this))
              .subscribe(() => {
                this.loadWorkflowWithId(wid);
              });
          } else {
            // no workflow to load, pending to create a new workflow
          }
          // responsible for persisting the workflow to the backend
          this.registerAutoPersistWorkflow();
        } else {
          // remember URL fragment
          const fragment = this.route.snapshot.fragment;
          // fetch the cached workflow first
          const cachedWorkflow = this.workflowCacheService.getCachedWorkflow();
          // responsible for saving the existing workflow in cache
          this.registerAutoCacheWorkFlow();
          // load the cached workflow
          this.workflowActionService.reloadWorkflow(cachedWorkflow);
          // set the URL fragment to previous value
          // because reloadWorkflow will highlight/unhighlight all elements
          // which will change the URL fragment
          this.router.navigate([], {
            relativeTo: this.route,
            fragment: fragment !== null ? fragment : undefined,
            preserveFragment: false,
          });
          // highlight the operator, comment box, or link in the URL fragment
          if (fragment) {
            if (this.workflowActionService.getTexeraGraph().hasElementWithID(fragment)) {
              this.workflowActionService.highlightElements(false, fragment);
            } else {
              this.notificationService.error(`Element ${fragment} doesn't exist`);
              // remove the fragment from the URL
              this.router.navigate([], { relativeTo: this.route });
            }
          }
          // clear stack
          this.undoRedoService.clearUndoStack();
          this.undoRedoService.clearRedoStack();
        }
      });
  }

  registerReEstablishWebsocketUponWIdChange() {
    this.workflowActionService
      .workflowMetaDataChanged()
      .pipe(
        switchMap(() => of(this.workflowActionService.getWorkflowMetadata().wid)),
        filter(isDefined),
        distinctUntilChanged()
      )
      .pipe(untilDestroyed(this))
      .subscribe(wid => {
        this.workflowWebsocketService.reopenWebsocket(wid);
      });
  }

  goBack(): void {
    this.location.back();
  }
}
