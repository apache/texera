import { Component, Input, OnInit } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { Observable, forkJoin } from "rxjs";
import { UserProjectService } from "src/app/dashboard/service/user-project/user-project.service";
import { DashboardWorkflowEntry } from "../../../../../type/dashboard-workflow-entry";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-remove-project-workflow-modal",
  templateUrl: "./ngbd-modal-remove-project-workflow.component.html",
  styleUrls: ["./ngbd-modal-remove-project-workflow.component.scss"]
})
export class NgbdModalRemoveProjectWorkflowComponent implements OnInit {
  @Input() addedWorkflows!: DashboardWorkflowEntry[];
  @Input() projectId!: number;

  public checkedWorkflows: boolean[] = [];

  constructor(
    public activeModal: NgbActiveModal,
    private userProjectService: UserProjectService
  ) { }

  ngOnInit(): void {
    this.checkedWorkflows = new Array(this.addedWorkflows.length).fill(false);
  }

  public submitForm() {
    let observables: Observable<Response>[] = [];

    for (let index = this.checkedWorkflows.length - 1; index >= 0; --index) {
      if (this.checkedWorkflows[index]) {
        observables.push(this.userProjectService.removeWorkflowFromProject(this.projectId, this.addedWorkflows[index].workflow.wid!));
        this.addedWorkflows.splice(index, 1); // for updating frontend cache
      }
    }

    forkJoin(observables)
       .pipe(untilDestroyed(this))
       .subscribe(response => {
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
}
