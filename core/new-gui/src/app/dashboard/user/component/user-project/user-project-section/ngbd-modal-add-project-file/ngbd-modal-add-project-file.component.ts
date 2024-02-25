import { Component, Input, OnInit } from "@angular/core";
import { forkJoin, Observable } from "rxjs";
import { UserFileService } from "../../../../service/user-file/user-file.service";
import { UserProjectService } from "../../../../service/user-project/user-project.service";
import { DashboardFile } from "../../../../type/dashboard-file.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-add-project-file-modal",
  templateUrl: "./ngbd-modal-add-project-file.component.html",
  styleUrls: ["./ngbd-modal-add-project-file.component.scss"],
})
export class NgbdModalAddProjectFileComponent implements OnInit {
  @Input() addedFiles!: ReadonlyArray<DashboardFile>;
  @Input() projectId!: number;

  public unaddedFiles: ReadonlyArray<DashboardFile> = [];
  public checkedFiles: boolean[] = [];
  private addedFileKeys: Set<number> = new Set<number>();

  constructor(
    private userFileService: UserFileService,
    private userProjectService: UserProjectService
  ) {}

  ngOnInit(): void {
    /* determine which files are already part of this project.
       this is used to filter which files are shown to the user */
    this.addedFiles.forEach(fileEntry => this.addedFileKeys.add(fileEntry.file.fid!));
    this.refreshProjectFileEntries();
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

  public submitForm() {
    let observables: Observable<Response>[] = [];

    for (let index = 0; index < this.checkedFiles.length; ++index) {
      if (this.checkedFiles[index]) {
        observables.push(this.userProjectService.addFileToProject(this.projectId, this.unaddedFiles[index].file.fid!));
      }
    }

    forkJoin(observables)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.userProjectService.refreshFilesOfProject(this.projectId));
  }

  public addFileSizeUnit(fileSize: number): string {
    return this.userFileService.addFileSizeUnit(fileSize);
  }

  private refreshProjectFileEntries(): void {
    this.userFileService
      .getFileList()
      .pipe(untilDestroyed(this))
      .subscribe(dashboardFileEntries => {
        this.unaddedFiles = dashboardFileEntries.filter(
          fileEntry => fileEntry.file.fid !== undefined && !this.addedFileKeys.has(fileEntry.file.fid!)
        );
        this.checkedFiles = new Array(this.unaddedFiles.length).fill(false);
      });
  }
}
