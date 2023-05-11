import { Component, Input, OnInit } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { FormBuilder, Validators } from "@angular/forms";
import { WorkflowAccessService } from "../../../../service/workflow-access/workflow-access.service";
import { WorkflowAccessEntry } from "../../../../type/access.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DashboardUserFileEntry, UserFile } from "../../../../type/dashboard-user-file-entry";
import { UserFileService } from "../../../../service/user-file/user-file.service";

@UntilDestroy()
@Component({
  selector: "texera-ngbd-modal-share-access",
  templateUrl: "./ngbd-modal-workflow-share-access.component.html",
  styleUrls: ["./ngbd-modal-workflow-share-access.component.scss"],
})
export class NgbdModalWorkflowShareAccessComponent implements OnInit {
  @Input() wid!: number;
  @Input() filenames!: string[];
  @Input() allOwners!: string[];

  validateForm = this.formBuilder.group({
    email: [null, [Validators.email, Validators.required]],
    accessLevel: ["READ"],
  });

  public accessList: ReadonlyArray<WorkflowAccessEntry> = [];
  public owner: string = "";
  public filteredOwners: Array<string> = [];
  public ownerSearchValue?: string;
  constructor(
    public activeModal: NgbActiveModal,
    private workflowAccessService: WorkflowAccessService,
    private formBuilder: FormBuilder,
    private userFileService: UserFileService
  ) {}

  ngOnInit(): void {
    this.workflowAccessService
      .getAccessList(this.wid)
      .pipe(untilDestroyed(this))
      .subscribe(access => (this.accessList = access));
    this.workflowAccessService
      .getOwner(this.wid)
      .pipe(untilDestroyed(this))
      .subscribe(name => {
        this.owner = name;
      });
  }

  public onChange(value: string): void {
    if (value === undefined) {
      this.filteredOwners = [];
    } else {
      this.filteredOwners = this.allOwners.filter(owner => owner.toLowerCase().indexOf(value.toLowerCase()) !== -1);
    }
  }

  public grantAccess(): void {
    if (this.validateForm.valid) {
      const userToShareWith = this.validateForm.get("email")?.value;
      const accessLevel = this.validateForm.get("accessLevel")?.value;
      if (this.filenames) {
        this.filenames.forEach(filename => {
          const [owner, fname] = filename.split("/", 2);
          let userFile: UserFile;
          userFile = {
            fid: undefined!,
            name: fname,
            path: undefined!,
            size: undefined!,
            description: undefined!,
            uploadTime: undefined!,
          };
          const dashboardUserFileEntry: DashboardUserFileEntry = {
            ownerName: owner,
            file: userFile,
            accessLevel: "read",
            isOwner: true,
            projectIDs: undefined!,
          };
          this.userFileService
            .grantUserFileAccess(dashboardUserFileEntry, userToShareWith, "read")
            .pipe(untilDestroyed(this))
            .subscribe();
        });
      }

      this.workflowAccessService
        .grantAccess(this.wid, userToShareWith, accessLevel)
        .pipe(untilDestroyed(this))
        .subscribe(() => this.ngOnInit());
    }
  }

  public revokeAccess(userToRemove: string): void {
    this.workflowAccessService
      .revokeAccess(this.wid, userToRemove)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.ngOnInit());
  }
}
