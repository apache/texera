import { Component, Input, OnInit } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormBuilder, Validators } from '@angular/forms';
import {
  UserWorkflowAccess,
  WorkflowGrantAccessService
} from '../../../../../common/service/user/workflow-access-control/workflow-grant-access.service';
import { Workflow } from '../../../../../common/type/workflow';


@Component({
  selector: 'texera-ngbd-modal-share-access',
  templateUrl: './ngbd-modal-share-access.component.html',
  styleUrls: ['./ngbd-modal-share-access.component.scss']
})
export class NgbdModalShareAccessComponent implements OnInit {

  @Input() workflow!: Workflow;

  shareForm = this.formBuilder.group({
    username: ['',[Validators.required]],
    accessLevel: ['', [Validators.required]]
  });

  accessLevels: string[] = ['read', 'write'];

  allUserWorkflowAccess: Readonly<UserWorkflowAccess>[] = [];


  public defaultWeb: String = 'http://localhost:4200/';

  constructor(
    public activeModal: NgbActiveModal,
    private workflowGrantAccessService: WorkflowGrantAccessService,
    private formBuilder: FormBuilder
  ) {
  }

  ngOnInit(): void {
    this.refreshGrantedList(this.workflow);
  }


  public onClickGetAllSharedAccess(workflow: Workflow): void {
    this.refreshGrantedList(workflow);
  }

  /**
   * get all shared access of the current workflow
   * @param workflow target/current workflow
   */
  public refreshGrantedList(workflow: Workflow): void {
    this.workflowGrantAccessService.retrieveGrantedList(workflow).subscribe(
      (userWorkflowAccess: Readonly<UserWorkflowAccess>[]) => this.allUserWorkflowAccess = userWorkflowAccess,
      err => console.log(err.error)
    );
  }

  /**
   * grant a specific level of access to a user
   * @param workflow the given/target workflow
   * @param userToShareWith the target user
   * @param accessLevel the type of access to be given
   */
  public grantAccess(workflow: Workflow, userToShareWith: string, accessLevel: string): void {
    this.workflowGrantAccessService.grantAccess(workflow, userToShareWith, accessLevel).subscribe(
      () => this.refreshGrantedList(workflow),
      err => alert(err.error));
  }


  /**
   * triggered by clicking the SUBMIT button, offers access based on the input information
   * @param workflow target/current workflow
   */
  public onClickShareWorkflow(workflow: Workflow): void {
    if(this.shareForm.get('username')?.invalid){
      alert("Please Fill in Username")
      return
    }
    if(this.shareForm.get('accessLevel')?.invalid){
      alert("Please Select Access Level")
      return
    }
    const userToShareWith = this.shareForm.get('username')?.value;
    const accessLevel = this.shareForm.get('accessLevel')?.value;
    this.grantAccess(workflow, userToShareWith, accessLevel);
  }

  /**
   * remove any type of access of the target used
   * @param workflow the given/target workflow
   * @param userToRemove the target user
   */
  public onClickRemoveAccess(workflow: Workflow, userToRemove: string): void {
    this.workflowGrantAccessService.revokeAccess(workflow, userToRemove).subscribe(
      () => this.refreshGrantedList(workflow),
      err => alert(err.error)
    );
  }

  /**
   * change form information based on user behavior on UI
   * @param e selected value
   */
  changeType(e: any) {
    this.shareForm.setValue({'accessLevel': e.target.value});
  }


}
