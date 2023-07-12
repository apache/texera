import { Component, Input, OnInit } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { PublicProjectService } from "../../../service/public-project/public-project.service";
import { DashboardProject } from "../../../type/dashboard-project.interface";

@UntilDestroy()
@Component({
  templateUrl: "public-project.component.html",
})
export class PublicProjectComponent implements OnInit {
  @Input() writeAccess!: boolean;
  userProjectEntries: DashboardProject[] = [];
  checked = false;
  indeterminate = false;
  checkedList = new Set<number>();
  constructor(
    public activeModal: NgbActiveModal,
    private publicProjectService: PublicProjectService
  ) {}

  ngOnInit(): void {
    this.publicProjectService
      .getPublicProjects()
      .pipe(untilDestroyed(this))
      .subscribe(userProjectEntries => {
        this.userProjectEntries = userProjectEntries;
      });
  }



  updateCheckedSet(id: number, checked: boolean): void {
    if (checked) {
      this.checkedList.add(id);
    } else {
      this.checkedList.delete(id);
    }
  }

  onItemChecked(id: number, checked: boolean): void {
    this.updateCheckedSet(id, checked);
    this.refreshCheckedStatus();
  }

  onAllChecked(value: boolean): void {
    this.userProjectEntries.forEach(item => this.updateCheckedSet(item.pid, value));
    this.refreshCheckedStatus();
  }

  refreshCheckedStatus(): void {
    this.checked = this.userProjectEntries.every(item => this.checkedList.has(item.pid));
    this.indeterminate = this.userProjectEntries.some(item => this.checkedList.has(item.pid)) && !this.checked;
  }
  addPublicProjects(): void {
    this.publicProjectService
      .addPublicProjects(Array.from(this.checkedList))
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.activeModal.close();
      });
  }
}
