import { Component, Input } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormBuilder, Validators } from '@angular/forms';
import { UserFileService } from '../../../../../common/service/user/user-file/user-file.service';
import { DashboardUserFileEntry, UserFileAccess } from '../../../../../common/type/dashboard-user-file-entry';


@Component({
  selector: 'texera-ngbd-modal-file-share-access',
  templateUrl: './ngbd-modal-file-share-access.component.html',
  styleUrls: ['./ngbd-modal-file-share-access.component.scss']
})
export class NgbdModalFileShareAccessComponent {

  @Input() dashboardUserFileEntry!: DashboardUserFileEntry;

  shareForm = this.formBuilder.group({
    username: ['', [Validators.required]],
    accessLevel: ['', [Validators.required]]
  });

  allUserFileAccess: ReadonlyArray<UserFileAccess> = [];

  accessLevels = ['read', 'write'];

  fileOwner = '';

  constructor(
    public activeModal: NgbActiveModal,
    private userFileService: UserFileService,
    private formBuilder: FormBuilder
  ) {
  }

  /**
   * get all shared access of the current dashboardUserFileEntry
   * @param dashboardUserFileEntry target/current dashboardUserFileEntry
   */
  public refreshGrantedUserFileAccessList(dashboardUserFileEntry: DashboardUserFileEntry): void {
    this.userFileService.getUserFileAccessList(dashboardUserFileEntry).subscribe(
      (userFileAccess: ReadonlyArray<UserFileAccess>) => {
        const newAccessList: UserFileAccess[] = [];
        userFileAccess.map(ufa => {
          if (ufa.accessLevel === 'Owner') { this.fileOwner = ufa.username; } else { newAccessList.push(ufa); }
        });
        this.allUserFileAccess = newAccessList;
      },
      err => console.log(err.error)
    );
  }




  /**
   * triggered by clicking the SUBMIT button, offers access based on the input information
   * @param dashboardUserFileEntry target/current file
   */
  public onClickShareUserFile(dashboardUserFileEntry: DashboardUserFileEntry): void {
    if (this.shareForm.get('username')?.invalid) {
      alert('Please Fill in Username');
      return;
    }
    if (this.shareForm.get('accessLevel')?.invalid) {
      alert('Please Select Access Level');
      return;
    }
    const userToShareWith = this.shareForm.get('username')?.value;
    const accessLevel = this.shareForm.get('accessLevel')?.value;
    this.userFileService.grantUserFileAccess(dashboardUserFileEntry, userToShareWith, accessLevel).subscribe(
      () => this.refreshGrantedUserFileAccessList(dashboardUserFileEntry),
      err => alert(err.error));
  }

  /**
   * Remove the given user's access to the target file.
   * @param dashboardUserFileEntry target/current file.
   * @param userNameToRemove
   */
  public onClickRemoveUserFileAccess(dashboardUserFileEntry: DashboardUserFileEntry, userNameToRemove: string): void {
    this.userFileService.revokeUserFileAccess(dashboardUserFileEntry, userNameToRemove).subscribe(
      () => this.refreshGrantedUserFileAccessList(dashboardUserFileEntry),
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
