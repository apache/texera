import { Component, Input, OnInit } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { Observable, forkJoin } from "rxjs";
import { concatMap } from "rxjs/operators";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import { DashboardWorkflowEntry } from "src/app/dashboard/type/dashboard-workflow-entry";
import { UserProjectService } from "src/app/dashboard/service/user-project/user-project.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-add-project-workflow-modal",
  templateUrl: "./ngbd-modal-add-project-workflow.component.html",
  styleUrls: ["./ngbd-modal-add-project-workflow.component.scss"],
})
export class NgbdModalAddProjectWorkflowComponent implements OnInit {
  @Input() projectId!: number;

  public unaddedWorkflows: DashboardWorkflowEntry[] = []; // tracks which workflows to display, the ones that have not yet been added to the project
  public checkedWorkflows: boolean[] = []; // used to implement check boxes
  private addedWorkflowKeys: Set<number> = new Set<number>(); // tracks which workflows to NOT display,  the workflow IDs of the workflows (if any) already inside the project
  private addedWorkflows: DashboardWorkflowEntry[] = []; // for passing back to update the frontend cache, stores the new workflow list including newly added workflows

  constructor(
    public activeModal: NgbActiveModal,
    private workflowPersistService: WorkflowPersistService,
    private userProjectService: UserProjectService
  ) {}

  ngOnInit(): void {
    this.refreshProjectWorkflowEntries();
  }

  public submitForm() {
    // data structure to track group of updates to make to backend
    let observables: Observable<Response>[] = [];

    // process any selected workflows, updating backend then frontend cache
    for (let index = 0; index < this.checkedWorkflows.length; ++index) {
      if (this.checkedWorkflows[index]) {
        // if workflow is checked
        observables.push(
          this.userProjectService.addWorkflowToProject(this.projectId, this.unaddedWorkflows[index].workflow.wid!)
        );
        this.addedWorkflows.push(this.unaddedWorkflows[index]); // for updating frontend cache
      }
    }

    // pass back data to update local cache after all changes propagated to backend
    forkJoin(observables)
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.activeModal.close(this.addedWorkflows);
      });
  }

  public changeAll() {
    if (this.isAllChecked()) {
      this.checkedWorkflows.fill(false);
    } else {
      this.checkedWorkflows.fill(true);
    }
  }

  public isAllChecked() {
    return this.checkedWorkflows.length > 0 && this.checkedWorkflows.every(isChecked => isChecked);
  }

  private refreshProjectWorkflowEntries(): void {
    this.userProjectService
      .retrieveWorkflowsOfProject(this.projectId)
      .pipe(
        concatMap((dashboardWorkflowEntries: DashboardWorkflowEntry[]) => {
          this.addedWorkflows = dashboardWorkflowEntries;
          dashboardWorkflowEntries.forEach(workflowEntry => this.addedWorkflowKeys.add(workflowEntry.workflow.wid!));
          return this.workflowPersistService.retrieveWorkflowsBySessionUser();
        }),
        untilDestroyed(this)
      )
      .subscribe(dashboardWorkflowEntries => {
        this.unaddedWorkflows = dashboardWorkflowEntries.filter(
          workflowEntry =>
            workflowEntry.workflow.wid !== undefined && !this.addedWorkflowKeys.has(workflowEntry.workflow.wid!)
        );
        this.checkedWorkflows = new Array(this.unaddedWorkflows.length).fill(false);
      });
  }
}
