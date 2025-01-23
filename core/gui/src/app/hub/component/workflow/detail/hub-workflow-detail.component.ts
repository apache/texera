import { AfterViewInit, Component, HostListener, Inject, OnDestroy, OnInit, Optional } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ActivatedRoute, Router } from "@angular/router";
import { UserService } from "../../../../common/service/user/user.service";
import { WorkflowActionService } from "../../../../workspace/service/workflow-graph/model/workflow-action.service";
import { throttleTime } from "rxjs/operators";
import { Workflow } from "../../../../common/type/workflow";
import { isDefined } from "../../../../common/util/predicate";
import { HubWorkflowService } from "../../../service/workflow/hub-workflow.service";
import { Role, User } from "src/app/common/type/user";
import { Location } from "@angular/common";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { DASHBOARD_HUB_WORKFLOW_RESULT, DASHBOARD_USER_WORKSPACE } from "../../../../app-routing.constant";

export const THROTTLE_TIME_MS = 1000;

@UntilDestroy()
@Component({
  selector: "texera-hub-workflow-detail",
  templateUrl: "hub-workflow-detail.component.html",
  styleUrls: ["hub-workflow-detail.component.scss"],
})
export class HubWorkflowDetailComponent implements AfterViewInit, OnDestroy, OnInit {
  isHub: boolean = false;
  workflowName: string = "";
  ownerName: string = "";
  workflowDescription: string = "";
  isLogin = this.userService.isLogin();
  isActivatedUser: boolean = false;
  isLiked: boolean = false;
  likeCount: number = 0;
  cloneCount: number = 0;
  displayPreciseViewCount = false;
  viewCount: number = 0;
  wid: number | undefined;
  protected readonly currentUser?: User;

  constructor(
    private userService: UserService,
    private workflowActionService: WorkflowActionService,
    private route: ActivatedRoute,
    private router: Router,
    private notificationService: NotificationService,
    private hubWorkflowService: HubWorkflowService,
    private workflowPersistService: WorkflowPersistService,
    private location: Location,
    @Optional() @Inject(NZ_MODAL_DATA) public input: { wid: number } | undefined
  ) {
    this.wid = input?.wid; //Accessing from the pop up. getting wid from the @Input
    if (!isDefined(this.wid)) {
      // otherwise getting wid from the route
      this.wid = this.route.snapshot.params.id;
      this.isHub = true;
    }
    this.currentUser = this.userService.getCurrentUser();
    if (this.currentUser?.role === Role.ADMIN || this.currentUser?.role === Role.REGULAR) {
      this.isActivatedUser = true;
    }
    this.workflowActionService.disableWorkflowModification();
  }

  ngOnInit() {
    if (!isDefined(this.wid)) {
      return;
    }

    // getting the workflow information
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
      .postViewWorkflow(this.wid, this.currentUser?.uid ?? 0)
      .pipe(throttleTime(THROTTLE_TIME_MS))
      .pipe(untilDestroyed(this))
      .subscribe(count => {
        this.viewCount = count;
      });
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

    // if there is a user, check if the user liked the workflow
    if (!isDefined(this.currentUser)) {
      return;
    }
    this.hubWorkflowService
      .isWorkflowLiked(this.wid, this.currentUser.uid)
      .pipe(untilDestroyed(this))
      .subscribe((isLiked: boolean) => {
        this.isLiked = isLiked;
      });
  }

  ngAfterViewInit(): void {
    if (!this.wid) {
      return;
    }
    this.loadWorkflowWithId(this.wid);
  }

  @HostListener("window:beforeunload")
  ngOnDestroy() {
    this.workflowActionService.clearWorkflow();
  }

  /**
   * Load the workflow with the given id.
   * If accessing through the hub, load the public workflow.
   * If accessing through the workspace, load the private workflow.
   * @param wid
   */
  loadWorkflowWithId(wid: number): void {
    if (!this.isHub) {
      this.workflowPersistService
        .retrieveWorkflow(wid)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (workflow: Workflow) => {
            // load the fetched workflow
            this.workflowActionService.reloadWorkflow(workflow);
          },
          error: () => {
            throw new Error(`Failed to load workflow with id ${wid}`);
          },
        });
    } else {
      this.hubWorkflowService
        .retrievePublicWorkflow(wid)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (workflow: Workflow) => {
            // load the fetched workflow
            this.workflowActionService.reloadWorkflow(workflow);
          },
          error: () => {
            throw new Error(`Failed to load workflow with id ${wid}`);
          },
        });
    }
  }

  goBack(): void {
    this.router.navigateByUrl(DASHBOARD_HUB_WORKFLOW_RESULT).catch(() => {
      this.notificationService.error("Go back failed. Please try again.");
    });
  }

  cloneWorkflow(): void {
    if (!isDefined(this.wid)) {
      return;
    }
    this.hubWorkflowService
      .cloneWorkflow(this.wid)
      .pipe(untilDestroyed(this))
      .subscribe(newWid => {
        this.router.navigate([`${DASHBOARD_USER_WORKSPACE}/${newWid}`]).then(() => {
          this.notificationService.success("Clone Successful");
        });
      });
  }

  toggleLike(): void {
    const userId = this.currentUser?.uid;
    if (!isDefined(userId) || !isDefined(this.wid)) {
      return;
    }

    if (this.isLiked) {
      this.hubWorkflowService
        .postUnlikeWorkflow(this.wid, userId)
        .pipe(untilDestroyed(this))
        .subscribe((success: boolean) => {
          if (success) {
            this.isLiked = false;
            if (!isDefined(this.wid)) {
              return;
            }
            this.hubWorkflowService
              .getLikeCount(this.wid)
              .pipe(untilDestroyed(this))
              .subscribe((count: number) => {
                this.likeCount = count;
              });
          }
        });
    } else {
      this.hubWorkflowService
        .postLikeWorkflow(this.wid, userId)
        .pipe(untilDestroyed(this))
        .subscribe((success: boolean) => {
          if (success) {
            this.isLiked = true;
            if (!isDefined(this.wid)) {
              return;
            }
            this.hubWorkflowService
              .getLikeCount(this.wid)
              .pipe(untilDestroyed(this))
              .subscribe((count: number) => {
                this.likeCount = count;
              });
          }
        });
    }
  }

  formatCount(count: number): string {
    if (count >= 1000) {
      return (count / 1000).toFixed(1) + "k";
    }
    return count.toString();
  }

  changeViewDisplayStyle() {
    this.displayPreciseViewCount = !this.displayPreciseViewCount;
  }
}
