import {
  AfterViewInit,
  Component,
  OnInit,
  HostListener,
  OnDestroy,
  ViewChild,
  ViewContainerRef,
  Input,
} from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ActivatedRoute, Router } from "@angular/router";
import { environment } from "../../../../../environments/environment";
import { UserService } from "../../../../common/service/user/user.service";
import { UndoRedoService } from "../../../../workspace/service/undo-redo/undo-redo.service";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { WorkflowWebsocketService } from "../../../../workspace/service/workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../../../../workspace/service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../../../workspace/service/operator-metadata/operator-metadata.service";
import { NzMessageService } from "ng-zorro-antd/message";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { CodeEditorService } from "../../../../workspace/service/code-editor/code-editor.service";
import { distinctUntilChanged, filter, switchMap, throttleTime } from "rxjs/operators";
import { Workflow } from "../../../../common/type/workflow";
import { of } from "rxjs";
import { isDefined } from "../../../../common/util/predicate";
import { HubWorkflowService } from "../../../service/workflow/hub-workflow.service";
import { User } from "src/app/common/type/user";
import { Location } from "@angular/common";

export const THROTTLE_TIME_MS = 1000;

@UntilDestroy()
@Component({
  selector: "texera-hub-workflow-detail",
  templateUrl: "hub-workflow-detail.component.html",
  styleUrls: ["hub-workflow-detail.component.scss"],
})
export class HubWorkflowDetailComponent implements AfterViewInit, OnDestroy, OnInit {
  isHub: boolean = true;
  workflowName: string = "";
  ownerName: string = "";
  workflowDescription: string = "";
  clonedWorklowId: number | undefined;
  isLogin = this.userService.isLogin();
  isLiked: boolean = false;
  currentUid: number | undefined;
  likeCount: number = 0;
  cloneCount: number = 0;
  displayPreciseViewCount = false;
  viewCount: number = 0;

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
  @Input() wid!: number;

  public pid?: number = undefined;
  userSystemEnabled = environment.userSystemEnabled;
  private currentUser?: User;
  @ViewChild("codeEditor", { read: ViewContainerRef }) codeEditorViewRef!: ViewContainerRef;
  constructor(
    private userService: UserService,
    // list additional services in constructor so they are initialized even if no one use them directly
    private undoRedoService: UndoRedoService,
    private workflowPersistService: WorkflowPersistService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService,
    private route: ActivatedRoute,
    private operatorMetadataService: OperatorMetadataService,
    private message: NzMessageService,
    private router: Router,
    private notificationService: NotificationService,
    private codeEditorService: CodeEditorService,
    private hubWorkflowService: HubWorkflowService,
    private location: Location
  ) {
    if (!this.wid) {
      this.wid = this.route.snapshot.params.id;
    }
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.isLogin = this.userService.isLogin();
      });
    this.currentUser = this.userService.getCurrentUser();
    this.currentUid = this.currentUser?.uid;
  }

  ngOnInit() {
    this.isHub =
      this.route.parent?.snapshot.url.some(segment => segment.path === "detail") ||
      this.route.snapshot.url.some(segment => segment.path === "detail");

    if (this.wid) {
      this.hubWorkflowService
        .getLikeCount(this.wid)
        .pipe(untilDestroyed(this))
        .subscribe(count => {
          this.likeCount = count;
        });

      this.hubWorkflowService
        .getCloneCount(this.wid)
        .pipe(untilDestroyed(this))
        .subscribe(count => {
          this.cloneCount = count;
        });

      this.hubWorkflowService
        .postViewWorkflow(this.wid, this.currentUid ? this.currentUid : 0)
        .pipe(throttleTime(THROTTLE_TIME_MS))
        .pipe(untilDestroyed(this))
        .subscribe(count => {
          this.viewCount = count;
        });
    }

    this.hubWorkflowService
      .getOwnerUser(this.wid)
      .pipe(untilDestroyed(this))
      .subscribe(owner => {
        this.ownerName = owner.name;
      });
    this.hubWorkflowService
      .getWorkflowName(this.wid)
      .pipe(untilDestroyed(this))
      .subscribe(workflowName => {
        this.workflowName = workflowName;
      });
    this.hubWorkflowService
      .getWorkflowDescription(this.wid)
      .pipe(untilDestroyed(this))
      .subscribe(workflowDescription => {
        this.workflowDescription = workflowDescription || "No description available";
      });
    if (this.wid !== undefined && this.currentUid != undefined) {
      this.hubWorkflowService
        .isWorkflowLiked(this.wid, this.currentUid)
        .pipe(untilDestroyed(this))
        .subscribe((isLiked: boolean) => {
          this.isLiked = isLiked;
        });
    }
  }

  ngAfterViewInit(): void {
    // clear the current workspace, reset as `WorkflowActionService.DEFAULT_WORKFLOW`
    this.workflowActionService.resetAsNewWorkflow();

    if (this.userSystemEnabled) {
      this.registerReEstablishWebsocketUponWIdChange();
    }

    this.registerLoadOperatorMetadata();

    this.codeEditorService.vc = this.codeEditorViewRef;
  }

  @HostListener("window:beforeunload")
  ngOnDestroy() {
    if (this.workflowPersistService.isWorkflowPersistEnabled()) {
      const workflow = this.workflowActionService.getWorkflow();
      if (this.isLogin) {
        this.workflowPersistService.persistWorkflow(workflow).pipe(untilDestroyed(this)).subscribe();
      }
    }

    this.codeEditorViewRef.clear();
    this.workflowWebsocketService.closeWebsocket();
    this.workflowActionService.clearWorkflow();
  }

  loadWorkflowWithId(wid: number): void {
    // disable the workspace until the workflow is fetched from the backend
    this.workflowActionService.disableWorkflowModification();
    let workflowObservable = this.currentUser
      ? this.workflowPersistService.retrieveWorkflow(wid)
      : this.hubWorkflowService.retrievePublicWorkflow(wid);
    workflowObservable.pipe(untilDestroyed(this)).subscribe(
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
        // load workflow with wid if presented in the URL
        if (this.wid) {
          // if wid is present in the url, load it from the backend
          this.userService
            .userChanged()
            .pipe(untilDestroyed(this))
            .subscribe(() => {
              this.loadWorkflowWithId(this.wid);
            });
        } else {
          // no workflow to load, pending to create a new workflow
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

  cloneWorkflow(): void {
    this.hubWorkflowService
      .cloneWorkflow(Number(this.wid))
      .pipe(untilDestroyed(this))
      .subscribe(newWid => {
        this.clonedWorklowId = newWid;
        sessionStorage.setItem("cloneSuccess", "true");
        this.router.navigate(["/dashboard/user/workflow"]);
      });
  }

  toggleLike(workflowId: number | undefined, userId: number | undefined): void {
    if (workflowId === undefined || userId === undefined) {
      return;
    }

    if (this.isLiked) {
      this.hubWorkflowService
        .postUnlikeWorkflow(workflowId, userId)
        .pipe(untilDestroyed(this))
        .subscribe((success: boolean) => {
          if (success) {
            this.isLiked = false;
            this.hubWorkflowService
              .getLikeCount(workflowId)
              .pipe(untilDestroyed(this))
              .subscribe((count: number) => {
                this.likeCount = count;
              });
            console.log("Successfully unliked the workflow");
          } else {
            console.error("Error unliking the workflow");
          }
        });
    } else {
      this.hubWorkflowService
        .postLikeWorkflow(workflowId, userId)
        .pipe(untilDestroyed(this))
        .subscribe((success: boolean) => {
          if (success) {
            this.isLiked = true;
            this.hubWorkflowService
              .getLikeCount(workflowId)
              .pipe(untilDestroyed(this))
              .subscribe((count: number) => {
                this.likeCount = count;
              });
            console.log("Successfully liked the workflow");
          } else {
            console.error("Error liking the workflow");
          }
        });
    }
  }

  formatLikeCount(count: number): string {
    if (count >= 1000) {
      return (count / 1000).toFixed(1) + "k";
    }
    return count.toString();
  }

  formatViewCount(count: number): string {
    if (!this.displayPreciseViewCount && count >= 1000) {
      return (count / 1000).toFixed(1) + "k";
    }
    return count.toString();
  }

  changeViewDisplayStyle() {
    this.displayPreciseViewCount = !this.displayPreciseViewCount;
  }
}
