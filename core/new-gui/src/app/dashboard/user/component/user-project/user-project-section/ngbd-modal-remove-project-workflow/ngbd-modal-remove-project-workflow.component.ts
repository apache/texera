import { Component, Input, OnInit } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { forkJoin, Observable } from "rxjs";
import { UserProjectService } from "src/app/dashboard/user/service/user-project/user-project.service";
import { DashboardWorkflow } from "../../../../type/dashboard-workflow.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-remove-project-workflow-modal",
  templateUrl: "./ngbd-modal-remove-project-workflow.component.html",
  styleUrls: ["./ngbd-modal-remove-project-workflow.component.scss"],
})
export class NgbdModalRemoveProjectWorkflowComponent implements OnInit {
  @Input() projectId!: number;

  public checkedWorkflows: boolean[] = []; // used to implement check boxes
  public addedWorkflows: DashboardWorkflow[] = []; // for passing back to update the frontend cache, stores the new workflow list with selected ones removed

  constructor(public activeModal: NgbActiveModal, private userProjectService: UserProjectService) {}

  ngOnInit(): void {
    this.refreshProjectWorkflowEntries();
  }

  public submitForm() {
    let observables: Observable<Response>[] = [];

    for (let index = this.checkedWorkflows.length - 1; index >= 0; --index) {
      if (this.checkedWorkflows[index]) {
        observables.push(
          this.userProjectService.removeWorkflowFromProject(this.projectId, this.addedWorkflows[index].workflow.wid!)
        );
        this.addedWorkflows.splice(index, 1); // for updating frontend cache
      }
    }

    forkJoin(observables)
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.activeModal.close(this.addedWorkflows);
      });
  }

  public isAllChecked() {
    return this.checkedWorkflows.length > 0 && this.checkedWorkflows.every(isChecked => isChecked);
  }

  public changeAll() {
    if (this.isAllChecked()) {
      this.checkedWorkflows.fill(false);
    } else {
      this.checkedWorkflows.fill(true);
    }
  }

  private refreshProjectWorkflowEntries(): void {
    this.userProjectService
      .retrieveWorkflowsOfProject(this.projectId)
      .pipe(untilDestroyed(this))
      .subscribe(dashboardWorkflowEntries => {
        this.addedWorkflows = dashboardWorkflowEntries;
        this.checkedWorkflows = new Array(this.addedWorkflows.length).fill(false);
      });
  }
}
