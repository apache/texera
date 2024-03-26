import { Component, inject, OnInit } from "@angular/core";
import { forkJoin, Observable } from "rxjs";
import { UserProjectService } from "../../../../service/user-project/user-project.service";
import { DashboardFile } from "../../../../type/dashboard-file.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UserFileService } from "../../../../service/user-file/user-file.service";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";

@UntilDestroy()
@Component({
  selector: "texera-remove-project-file-modal",
  templateUrl: "./ngbd-modal-remove-project-file.component.html",
  styleUrls: ["./ngbd-modal-remove-project-file.component.scss"],
})
export class NgbdModalRemoveProjectFileComponent implements OnInit {
  readonly addedFiles: ReadonlyArray<DashboardFile> = inject(NZ_MODAL_DATA).addedFiles;
  readonly projectId: number = inject(NZ_MODAL_DATA).projectId;

  public checkedFiles: boolean[] = [];

  constructor(
    private userProjectService: UserProjectService,
    private userFileService: UserFileService
  ) {}

  ngOnInit(): void {
    this.checkedFiles = new Array(this.addedFiles.length).fill(false);
  }

  public submitForm() {
    let observables: Observable<Response>[] = [];

    for (let index = this.checkedFiles.length - 1; index >= 0; --index) {
      if (this.checkedFiles[index]) {
        observables.push(
          this.userProjectService.removeFileFromProject(this.projectId, this.addedFiles[index].file.fid!)
        );
      }
    }

    forkJoin(observables)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.userProjectService.refreshFilesOfProject(this.projectId));
  }

  public isAllChecked() {
    return this.checkedFiles.length > 0 && this.checkedFiles.every(isChecked => isChecked);
  }

  public changeAll() {
    if (this.isAllChecked()) {
      this.checkedFiles.fill(false);
    } else {
      this.checkedFiles.fill(true);
    }
  }

  public addFileSizeUnit(fileSize: number): string {
    return this.userFileService.addFileSizeUnit(fileSize);
  }
}
