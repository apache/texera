import { Component, OnInit } from '@angular/core';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { NgbdModalFileAddComponent } from './ngbd-modal-file-add/ngbd-modal-file-add.component';
import { UserFileService } from '../../../service/user-file/user-file.service';
import { UserFile } from '../../../type/user-file';
import { UserService } from '../../.../../../../common/service/user/user.service';

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
      this.userFileService.refreshFiles();
    }

  ngOnInit() {
  }

  public openFileAddComponent() {
    const modalRef = this.modalService.open(NgbdModalFileAddComponent);
  }

  public getFileArray(): UserFile[] {
    return this.userFileService.getFileArray();
  }

  public getFileArrayLength(): number {
    return this.userFileService.getFileArrayLength();
  }

  public deleteFile(userFile: UserFile): void {
    this.userFileService.deleteFile(userFile);
  }

  public disableAddButton(): boolean {
    return !this.userService.isLogin();
  }

  public addFileSizeUnit(fileSize: number): string {
    return this.userFileService.addFileSizeUnit(fileSize);
  }

}
