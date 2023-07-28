import { Component, Input, OnInit } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { PublicProjectService } from "../../../service/public-project/public-project.service";
import { PublicProject } from "../../../type/dashboard-project.interface";

@UntilDestroy()
@Component({
  templateUrl: "public-project.component.html",
})
export class PublicProjectComponent implements OnInit {
  @Input() disabledList: Set<number> = new Set<number>();
  publicProjectEntries: PublicProject[] = [];
  checked = false;
  indeterminate = false;
  checkedList = new Set<number>();
  constructor(public activeModal: NgbActiveModal, private publicProjectService: PublicProjectService) {}

  ngOnInit(): void {
    this.publicProjectService
      .getPublicProjects()
      .pipe(untilDestroyed(this))
      .subscribe(publicProjects => (this.publicProjectEntries = publicProjects));
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
    this.publicProjectEntries.forEach(item => this.updateCheckedSet(item.pid, value));
    this.refreshCheckedStatus();
  }

  refreshCheckedStatus(): void {
    this.checked = this.publicProjectEntries.every(item => this.checkedList.has(item.pid));
    this.indeterminate = this.publicProjectEntries.some(item => this.checkedList.has(item.pid)) && !this.checked;
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
