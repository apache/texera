import { Component, Input, OnInit } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { FormBuilder, Validators } from "@angular/forms";
import { WorkflowAccessService } from "../../../../service/workflow-access/workflow-access.service";
import { Workflow } from "../../../../../common/type/workflow";
import { AccessEntry } from "../../../../type/access.interface";
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
  @Input() workflow!: Workflow;
  @Input() filenames!: string[];
  @Input() allOwners!: string[];

  validateForm = this.formBuilder.group({
    email: [null, [Validators.email, Validators.required]],
    accessLevel: ["read"],
  });

  public allUserWorkflowAccess: ReadonlyArray<AccessEntry> = [];
  public workflowOwner: string = "";
  public filteredOwners: Array<string> = [];
  public ownerSearchValue?: string;
  constructor(
    public activeModal: NgbActiveModal,
    private workflowGrantAccessService: WorkflowAccessService,
    private formBuilder: FormBuilder,
    private userFileService: UserFileService
  ) {}

  ngOnInit(): void {
    this.workflowGrantAccessService
      .retrieveGrantedWorkflowAccessList(this.workflow)
      .pipe(untilDestroyed(this))
      .subscribe(
        (userWorkflowAccess: ReadonlyArray<AccessEntry>) => (this.allUserWorkflowAccess = userWorkflowAccess),
        // @ts-ignore // TODO: fix this with notification component
        (err: unknown) => console.log(err.error)
      );
    this.workflowGrantAccessService
      .getWorkflowOwner(this.workflow)
      .pipe(untilDestroyed(this))
      .subscribe(({ ownerName }) => {
        this.workflowOwner = ownerName;
      });
  }

  public onChange(value: string): void {
    if (value === undefined) {
      this.filteredOwners = [];
    } else {
      this.filteredOwners = this.allOwners.filter(owner => owner.toLowerCase().indexOf(value.toLowerCase()) !== -1);
    }
  }

  /**
   * triggered by clicking the SUBMIT button, offers access based on the input information
   * @param workflow target/current workflow
   */
  public share(workflow: Workflow): void {
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
            .subscribe(
              // @ts-ignore // TODO: fix this with notification component
              (err: unknown) => alert(err.error)
            );
        });
      }

      this.workflowGrantAccessService
        .grantUserWorkflowAccess(workflow, userToShareWith, accessLevel)
        .pipe(untilDestroyed(this))
        .subscribe(
          () => this.ngOnInit(),
          // @ts-ignore // TODO: fix this with notification component
          (err: unknown) => alert(err.error)
        );
    }
  }

  /**
   * remove any type of access of the target used
   * @param workflow the given/target workflow
   * @param userToRemove the target user
   */
  public onClickRemoveAccess(workflow: Workflow, userToRemove: string): void {
    this.workflowGrantAccessService
      .revokeWorkflowAccess(workflow, userToRemove)
      .pipe(untilDestroyed(this))
      .subscribe(
        () => this.ngOnInit(),
        // @ts-ignore // TODO: fix this with notification component
        (err: unknown) => alert(err.error)
      );
  }
}
