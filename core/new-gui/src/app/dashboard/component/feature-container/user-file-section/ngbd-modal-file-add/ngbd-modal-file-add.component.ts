import { Component, OnInit } from '@angular/core';
import { FileUploader } from 'ng2-file-upload';
import { UserFileUploadService } from '../../../../../common/service/user/user-file/user-file-upload.service';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FileUploadItem } from '../../../../../common/type/user-file';

@Component({
  selector: 'texera-ngbd-modal-file-add',
  templateUrl: './ngbd-modal-file-add.component.html',
  styleUrls: ['./ngbd-modal-file-add.component.scss'],
})
export class NgbdModalFileAddComponent implements OnInit {

  // This checks whether the user has hover a file over the file upload area
  public haveDropZoneOver: boolean = false;

  // uploader is a data type introduced in ng2-uploader library, which can be used to capture files and store them
  //  inside the uploader queue.
  public uploader: FileUploader = new FileUploader({ url: '' });


  constructor(
    public activeModal: NgbActiveModal,
    private userFileUploadService: UserFileUploadService
  ) { }

  ngOnInit() {
  }

  public getFileArray(): ReadonlyArray<Readonly<FileUploadItem>> {
    return this.userFileUploadService.getFilesToBeUploaded();
  }

  public getFileArrayLength(): number {
    return this.userFileUploadService.getFilesToBeUploaded().length;
  }

  public deleteFile(fileUploadItem: FileUploadItem): void {
    this.userFileUploadService.removeFileFromUploadArray(fileUploadItem);
  }

  public uploadAllFiles(): void {
    this.userFileUploadService.uploadAllFiles();
  }

  public isUploadAllButtonDisabled(): boolean {
    return this.userFileUploadService.getFilesToBeUploaded().every(fileUploadItem => fileUploadItem.isUploadingFlag);
  }

  public haveFileOver(fileOverEvent: boolean): void {
    this.haveDropZoneOver = fileOverEvent;
  }

  public getFileDropped(fileDropEvent: FileList): void {
    for (let i = 0; i < fileDropEvent.length; i++) {
      const fileOrNull: File | null = fileDropEvent.item(i);
      if (this.isFile(fileOrNull)) {
        this.userFileUploadService.addFileToUploadArray(fileOrNull);
      }
    }

    this.uploader.clearQueue();
  }

  public handleClickUploadFile(clickUploadEvent: { target: HTMLInputElement }): void {
    const fileList: FileList | null = clickUploadEvent.target.files;
    if (fileList === null) {
      throw new Error(`browser upload does not work as intended`);
    }

    for (let i = 0; i < fileList.length; i++) {
      this.userFileUploadService.addFileToUploadArray(fileList[i]);
    }
  }

  private isFile(file: File | null): file is File {
    return file != null;
  }


}
