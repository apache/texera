import { Component, OnInit } from '@angular/core';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { NgbdModalFileAddComponent } from './ngbd-modal-file-add/ngbd-modal-file-add.component';
import { UserFileService } from '../../../service/user-file/user-file.service';
import { DashboardUserFileEntry } from '../../../type/dashboard-user-file-entry';
import { UserService } from '../../../../common/service/user/user.service';
import { NgbdModalUserFileShareAccessComponent } from './ngbd-modal-file-share-access/ngbd-modal-user-file-share-access.component';

@Component({
  selector: 'texera-user-file-section',
  templateUrl: './user-file-section.component.html',
  styleUrls: ['./user-file-section.component.scss']
})
export class UserFileSectionComponent implements OnInit {

  constructor(
    private modalService: NgbModal,
    private userFileService: UserFileService,
    private userService: UserService
  ) {
    this.userFileService.refreshDashboardUserFileEntries();
  }

  ngOnInit() {
  }

  public openFileAddComponent() {
    this.modalService.open(NgbdModalFileAddComponent);
  }

  public onClickOpenShareAccess(dashboardUserFileEntry: DashboardUserFileEntry): void {
    const modalRef = this.modalService.open(NgbdModalUserFileShareAccessComponent);
    modalRef.componentInstance.dashboardUserFileEntry = dashboardUserFileEntry;
  }

  public getFileArray(): ReadonlyArray<DashboardUserFileEntry> {
    const fileArray = this.userFileService.getUserFiles();
    if (!fileArray) {
      return [];
    }
    return fileArray;
  }

  public deleteUserFileEntry(userFileEntry: DashboardUserFileEntry): void {
    this.userFileService.deleteDashboardUserFileEntry(userFileEntry);
  }

  public disableAddButton(): boolean {
    return !this.userService.isLogin();
  }

  public addFileSizeUnit(fileSize: number): string {
    return this.userFileService.addFileSizeUnit(fileSize);
  }

  public downloadUserFile(userFileEntry: DashboardUserFileEntry): void {
    this.userFileService.downloadUserFile(userFileEntry.file);
  }
}
