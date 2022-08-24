import { Component, Input, OnInit } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { FormBuilder, Validators } from "@angular/forms";
import { WorkflowAccessService } from "../../../../service/workflow-access/workflow-access.service";
import { Workflow } from "../../../../../common/type/workflow";
import { AccessEntry } from "../../../../type/access.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowAccessLevel } from "src/app/dashboard/type/dashboard-workflow-entry";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { WorkflowMetadata } from "src/app/dashboard/type/workflow-metadata.interface";
import { HttpClient } from "@angular/common/http";
import { AppSettings } from "src/app/common/app-setting";


export const WORKFLOW_ACCESS_URL = `${AppSettings.getApiEndpoint()}/workflow/access`;
export const WORKFLOW_ACCESS_GRANT_URL = WORKFLOW_ACCESS_URL + "/grant";
export const WORKFLOW_ACCESS_LIST_URL = WORKFLOW_ACCESS_URL + "/list";
export const WORKFLOW_OWNER_URL = WORKFLOW_ACCESS_URL + "/owner";

@UntilDestroy()
@Component({
  selector: "texera-ngbd-modal-share-access",
  templateUrl: "./ngbd-modal-workflow-share-access.component.html",
  styleUrls: ["./ngbd-modal-workflow-share-access.component.scss"],
})
export class NgbdModalWorkflowShareAccessComponent implements OnInit {
  @Input() workflow: WorkflowMetadata | undefined;

  public shareForm = this.formBuilder.group({
    username: ["", [Validators.required]],
    accessLevel: ["", [Validators.required]],
  });

  public accessLevels: string[] = ["read", "write", "execute"];

  public allUserWorkflowAccess: ReadonlyArray<AccessEntry> = [];

  public workflowOwnerName?: string;

  constructor(
    public activeModal: NgbActiveModal,
    private workflowGrantAccessService: WorkflowAccessService,
    private formBuilder: FormBuilder,
  ) {}

  ngOnInit(): void {
    this.refreshGrantedList(this.workflow);
  }

  public onClickGetAllSharedAccess(workflow: WorkflowMetadata): void {
    this.refreshGrantedList(workflow);
  }

  /**
   * get all shared access of the current workflow
   * @param workflow target/current workflow
   */
  public refreshGrantedList(workflow?: WorkflowMetadata): void {
    if (! workflow) {
      return;
    }
    this.workflowGrantAccessService
      .retrieveGrantedWorkflowAccessList(workflow)
      .pipe(untilDestroyed(this))
      .subscribe(
        (userWorkflowAccess: ReadonlyArray<AccessEntry>) => (this.allUserWorkflowAccess = userWorkflowAccess),
        // @ts-ignore // TODO: fix this with notification component
        (err: unknown) => console.log(err.error)
      );
    this.workflowGrantAccessService
      .getWorkflowOwner(workflow)
      .pipe(untilDestroyed(this))
      .subscribe(ownerName => {
        this.workflowOwnerName = ownerName;
      });
  }

  /**
   * grant a specific level of access to a user
   * @param workflow the given/target workflow
   * @param userToShareWith the target user
   * @param accessLevel the type of access to be given
   */
  public grantWorkflowAccess(workflow: WorkflowMetadata, userToShareWith: string, accessLevel: WorkflowAccessLevel): void {
    this.workflowGrantAccessService
      .grantUserWorkflowAccess(workflow, userToShareWith, accessLevel)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.refreshGrantedList(workflow));
  }

  /**
   * triggered by clicking the SUBMIT button, offers access based on the input information
   * @param workflow target/current workflow
   */
  public onClickShareWorkflow(workflow: WorkflowMetadata): void {
    if (this.shareForm.get("username")?.invalid) {
      alert("Please Fill in Username");
      return;
    }
    if (this.shareForm.get("accessLevel")?.invalid) {
      alert("Please Select Access Level");
      return;
    }
    const userToShareWith = this.shareForm.get("username")?.value;
    const accessLevel = this.shareForm.get("accessLevel")?.value;
    this.grantWorkflowAccess(workflow, userToShareWith, accessLevel);
  }

  /**
   * remove any type of access of the target used
   * @param workflow the given/target workflow
   * @param userToRemove the target user
   */
  public onClickRemoveAccess(workflow: WorkflowMetadata, userToRemove: string): void {
    if (! workflow) {
      return;
    }
    this.workflowGrantAccessService
      .grantUserWorkflowAccess(workflow, userToRemove, 'none')
      .pipe(untilDestroyed(this))
      .subscribe(() => this.refreshGrantedList(workflow));
  }

  /**
   * change form information based on user behavior on UI
   * @param e selected value
   */
  changeType(e: any) {
    this.shareForm.setValue({ accessLevel: e.target.value });
  }
}
