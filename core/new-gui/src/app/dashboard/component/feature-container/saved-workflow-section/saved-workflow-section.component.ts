import { Component, OnInit } from "@angular/core";
import { Router } from "@angular/router";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { cloneDeep } from "lodash-es";
import { from } from "rxjs";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { NgbdModalDeleteWorkflowComponent } from "./ngbd-modal-delete-workflow/ngbd-modal-delete-workflow.component";
import { NgbdModalWorkflowShareAccessComponent } from "./ngbd-modal-share-access/ngbd-modal-workflow-share-access.component";
import { DashboardWorkflowEntry } from "../../../type/dashboard-workflow-entry";
import { UserService } from "../../../../common/service/user/user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import Fuse from "fuse.js";
import { NgbdModalWorkflowExecutionsComponent } from "./ngbd-modal-workflow-executions/ngbd-modal-workflow-executions.component";
import { environment } from "../../../../../environments/environment";

export const ROUTER_WORKFLOW_BASE_URL = "/workflow";
export const ROUTER_WORKFLOW_CREATE_NEW_URL = "/";

@UntilDestroy()
@Component({
  selector: "texera-saved-workflow-section",
  templateUrl: "./saved-workflow-section.component.html",
  styleUrls: ["./saved-workflow-section.component.scss", "../../dashboard.component.scss"],
})
export class SavedWorkflowSectionComponent implements OnInit {
  // virtual scroll requires replacing the entire array reference in order to update view
  // see https://github.com/angular/components/issues/14635
  public dashboardWorkflowEntries: ReadonlyArray<DashboardWorkflowEntry> = [];
  public dashboardWorkflowEntriesIsEditingName: number[] = [];
  public allDashboardWorkflowEntries: DashboardWorkflowEntry[] = [];
  public filteredDashboardWorkflowNames: Array<string> = [];
  public fuse = new Fuse([] as ReadonlyArray<DashboardWorkflowEntry>, {
    shouldSort: true,
    threshold: 0.2,
    location: 0,
    distance: 100,
    minMatchCharLength: 1,
    keys: ["workflow.wid", "workflow.name", "ownerName"],
  });
  public searchCriteriaPathMapping: Map<string, string[]> = new Map([
    ["workflowName", ["workflow", "name"]],
    ["id", ["workflow", "wid"]],
    ["owner", ["ownerName"]],
  ]);
  public workflowSearchValue: string = "";
  private defaultWorkflowName: string = "Untitled Workflow";
  public searchCriteria: string[] = ["owner", "id"];
  // whether tracking metadata information about executions is enabled
  public workflowExecutionsTrackingEnabled: boolean = environment.workflowExecutionsTrackingEnabled;

  constructor(
    private userService: UserService,
    private workflowPersistService: WorkflowPersistService,
    private notificationService: NotificationService,
    private modalService: NgbModal,
    private router: Router
  ) {}

  ngOnInit() {
    this.registerDashboardWorkflowEntriesRefresh();
  }

  /**
   * open the Modal based on the workflow clicked on
   */
  public onClickOpenShareAccess({ workflow }: DashboardWorkflowEntry): void {
    const modalRef = this.modalService.open(NgbdModalWorkflowShareAccessComponent);
    modalRef.componentInstance.workflow = workflow;
  }

  /**
   * open the workflow executions page
   */
  public onClickGetWorkflowExecutions({ workflow }: DashboardWorkflowEntry): void {
    const modalRef = this.modalService.open(NgbdModalWorkflowExecutionsComponent, {
      size: "lg",
      windowClass: "modal-xl",
    });
    modalRef.componentInstance.workflow = workflow;
  }

  public searchInputOnChange(value: string): void {
    // enable autocomplete only when searching for workflow name
    if (!value.includes(":")) {
      const filteredDashboardWorkflowNames: string[] = [];
      this.allDashboardWorkflowEntries.forEach(dashboardEntry => {
        const workflowName = dashboardEntry.workflow.name;
        if (workflowName.toLowerCase().indexOf(value.toLowerCase()) !== -1) {
          filteredDashboardWorkflowNames.push(workflowName);
        }
      });
      this.filteredDashboardWorkflowNames = filteredDashboardWorkflowNames;
    }
  }

  // check https://fusejs.io/api/query.html#logical-query-operators for logical query operators rule
  public buildAndPathQuery(
    workflowSearchField: string,
    workflowSearchValue: string
  ): {
    $path: ReadonlyArray<string>;
    $val: string;
  } {
    return {
      $path: this.searchCriteriaPathMapping.get(workflowSearchField) as ReadonlyArray<string>,
      $val: workflowSearchValue,
    };
  }

  /**
   * Search workflows by owner name, workflow name or workflow id
   * Use fuse.js https://fusejs.io/ as the tool for searching
   */
  public searchWorkflow(): void {
    let andPathQuery: Object[] = [];
    // empty search value, return all workflow entries
    if (this.workflowSearchValue.trim() === "") {
      this.dashboardWorkflowEntries = [...this.allDashboardWorkflowEntries];
      return;
    } else if (!this.workflowSearchValue.includes(":")) {
      // search only by workflow name
      andPathQuery.push(this.buildAndPathQuery("workflowName", this.workflowSearchValue));
      this.dashboardWorkflowEntries = this.fuse.search({ $and: andPathQuery }).map(res => res.item);
      return;
    }
    const searchConsitionsSet = new Set(this.workflowSearchValue.trim().split(/ +(?=(?:(?:[^"]*"){2})*[^"]*$)/g));
    searchConsitionsSet.forEach(condition => {
      // field search
      if (condition.includes(":")) {
        const conditionArray = condition.split(":");
        if (conditionArray.length !== 2) {
          this.notificationService.error("Please check the format of the search query");
          return;
        }
        const workflowSearchField = conditionArray[0];
        const workflowSearchValue = conditionArray[1];
        if (!this.searchCriteria.includes(workflowSearchField)) {
          this.notificationService.error("Cannot search by " + workflowSearchField);
          return;
        }
        andPathQuery.push(this.buildAndPathQuery(workflowSearchField, workflowSearchValue));
      } else {
        //search by workflow name
        andPathQuery.push(this.buildAndPathQuery("workflowName", condition));
      }
    });
    this.dashboardWorkflowEntries = this.fuse.search({ $and: andPathQuery }).map(res => res.item);
  }

  /**
   * sort the workflow by name in ascending order
   */
  public ascSort(): void {
    this.dashboardWorkflowEntries = this.dashboardWorkflowEntries
      .slice()
      .sort((t1, t2) => t1.workflow.name.toLowerCase().localeCompare(t2.workflow.name.toLowerCase()));
  }

  /**
   * sort the project by name in descending order
   */
  public dscSort(): void {
    this.dashboardWorkflowEntries = this.dashboardWorkflowEntries
      .slice()
      .sort((t1, t2) => t2.workflow.name.toLowerCase().localeCompare(t1.workflow.name.toLowerCase()));
  }

  /**
   * sort the project by creating time
   */
  public dateSort(): void {
    this.dashboardWorkflowEntries = this.dashboardWorkflowEntries
      .slice()
      .sort((left, right) =>
        left.workflow.creationTime !== undefined && right.workflow.creationTime !== undefined
          ? left.workflow.creationTime - right.workflow.creationTime
          : 0
      );
  }

  /**
   * sort the project by last modified time
   */
  public lastSort(): void {
    this.dashboardWorkflowEntries = this.dashboardWorkflowEntries
      .slice()
      .sort((left, right) =>
        left.workflow.lastModifiedTime !== undefined && right.workflow.lastModifiedTime !== undefined
          ? left.workflow.lastModifiedTime - right.workflow.lastModifiedTime
          : 0
      );
  }

  /**
   * create a new workflow. will redirect to a pre-emptied workspace
   */
  public onClickCreateNewWorkflowFromDashboard(): void {
    this.router.navigate([`${ROUTER_WORKFLOW_CREATE_NEW_URL}`]).then(null);
  }

  /**
   * duplicate the current workflow. A new record will appear in frontend
   * workflow list and backend database.
   */
  public onClickDuplicateWorkflow({ workflow: { wid } }: DashboardWorkflowEntry): void {
    if (wid) {
      this.workflowPersistService
        .duplicateWorkflow(wid)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: duplicatedWorkflowInfo => {
            this.dashboardWorkflowEntries = [...this.dashboardWorkflowEntries, duplicatedWorkflowInfo];
          },
          error: (err: unknown) => alert(err),
        });
    }
  }

  /**
   * openNgbdModalDeleteWorkflowComponent trigger the delete workflow
   * component. If user confirms the deletion, the method sends
   * message to frontend and delete the workflow on frontend. It
   * calls the deleteProject method in service which implements backend API.
   */
  public openNgbdModalDeleteWorkflowComponent({ workflow }: DashboardWorkflowEntry): void {
    const modalRef = this.modalService.open(NgbdModalDeleteWorkflowComponent);
    modalRef.componentInstance.workflow = cloneDeep(workflow);

    from(modalRef.result)
      .pipe(untilDestroyed(this))
      .subscribe((confirmToDelete: boolean) => {
        const wid = workflow.wid;
        if (confirmToDelete && wid !== undefined) {
          this.workflowPersistService
            .deleteWorkflow(wid)
            .pipe(untilDestroyed(this))
            .subscribe(
              _ => {
                this.dashboardWorkflowEntries = this.dashboardWorkflowEntries.filter(
                  workflowEntry => workflowEntry.workflow.wid !== wid
                );
              },
              // @ts-ignore // TODO: fix this with notification component
              (err: unknown) => alert(err.error)
            );
        }
      });
  }

  /**
   * jump to the target workflow canvas
   */
  public jumpToWorkflow({ workflow: { wid } }: DashboardWorkflowEntry): void {
    this.router.navigate([`${ROUTER_WORKFLOW_BASE_URL}/${wid}`]).then(null);
  }

  private registerDashboardWorkflowEntriesRefresh(): void {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.userService.isLogin()) {
          this.refreshDashboardWorkflowEntries();
        } else {
          this.clearDashboardWorkflowEntries();
        }
      });
  }

  private refreshDashboardWorkflowEntries(): void {
    this.workflowPersistService
      .retrieveWorkflowsBySessionUser()
      .pipe(untilDestroyed(this))
      .subscribe(dashboardWorkflowEntries => {
        this.dashboardWorkflowEntries = dashboardWorkflowEntries;
        this.allDashboardWorkflowEntries = dashboardWorkflowEntries;
        this.fuse.setCollection(this.allDashboardWorkflowEntries);
        const newEntries = dashboardWorkflowEntries.map(e => e.workflow.name);
        this.filteredDashboardWorkflowNames = [...this.filteredDashboardWorkflowNames, ...newEntries];
      });
  }

  private clearDashboardWorkflowEntries(): void {
    this.dashboardWorkflowEntries = [];
  }

  public confirmUpdateWorkflowCustomName(
    dashboardWorkflowEntry: DashboardWorkflowEntry,
    name: string,
    index: number
  ): void {
    const { workflow } = dashboardWorkflowEntry;
    this.workflowPersistService
      .updateWorkflowName(workflow.wid, name || this.defaultWorkflowName)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        let updatedDashboardWorkFlowEntry = { ...dashboardWorkflowEntry };
        updatedDashboardWorkFlowEntry.workflow = { ...workflow };
        updatedDashboardWorkFlowEntry.workflow.name = name || this.defaultWorkflowName;
        const newEntries = this.dashboardWorkflowEntries.slice();
        newEntries[index] = updatedDashboardWorkFlowEntry;
        this.dashboardWorkflowEntries = newEntries;
      })
      .add(() => {
        this.dashboardWorkflowEntriesIsEditingName = this.dashboardWorkflowEntriesIsEditingName.filter(
          entryIsEditingIndex => entryIsEditingIndex != index
        );
      });
  }
}
