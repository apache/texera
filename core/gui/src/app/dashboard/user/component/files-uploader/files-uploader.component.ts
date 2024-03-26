import { Component, EventEmitter, Input, OnInit, Output } from "@angular/core";
import { FileUploadItem } from "../../type/dashboard-file.interface";
import { NgxFileDropEntry } from "ngx-file-drop";
import { UserFileUploadService } from "../../service/user-file/user-file-upload.service";
import {
  DatasetVersionFileTreeManager,
  DatasetVersionFileTreeNode,
  getFullPathFromFileTreeNode,
  getPathsFromTreeNode,
  parseFileUploadItemToTreeNodes,
} from "../../../../common/type/datasetVersionFileTree";
import { NzUploadChangeParam, NzUploadFile } from "ng-zorro-antd/upload";

@Component({
  selector: "texera-user-files-uploader",
  templateUrl: "./files-uploader.component.html",
  styleUrls: ["./files-uploader.component.scss"],
})
export class FilesUploaderComponent {
  @Input()
  previouslyUploadFiles: DatasetVersionFileTreeNode[] | undefined;
  previouslyUploadFilesManager: DatasetVersionFileTreeManager | undefined;

  @Output()
  uploadedFiles = new EventEmitter<FileUploadItem[]>();
  //
  @Output()
  removingFilePaths = new EventEmitter<string[]>();

  newUploadNodeToFileItems: Map<DatasetVersionFileTreeNode, FileUploadItem> = new Map<
    DatasetVersionFileTreeNode,
    FileUploadItem
  >();
  newUploadFileTreeManager: DatasetVersionFileTreeManager = new DatasetVersionFileTreeManager();
  newUploadFileTreeNodes: DatasetVersionFileTreeNode[] = [];

  fileUploadingFinished: boolean = false;
  // four types: "success", "info", "warning" and "error"
  fileUploadBannerType: "error" | "success" | "info" | "warning" = "success";
  fileUploadBannerMessage: string = "";

  hideBanner() {
    this.fileUploadingFinished = false;
  }

  showFileUploadBanner(bannerType: "error" | "success" | "info" | "warning", bannerMessage: string) {
    this.fileUploadingFinished = true;
    this.fileUploadBannerType = bannerType;
    this.fileUploadBannerMessage = bannerMessage;
  }

  public fileDropped(files: NgxFileDropEntry[]) {
    // here I use promise to ensure the atomicity of files uploading
    const filePromises = files.map(droppedFile => {
      return new Promise<FileUploadItem | null>((resolve, reject) => {
        if (droppedFile.fileEntry.isFile) {
          const fileEntry = droppedFile.fileEntry as FileSystemFileEntry;
          fileEntry.file(file => {
            const fileUploadItem = UserFileUploadService.createFileUploadItemWithPath(file, droppedFile.relativePath);
            resolve(fileUploadItem);
          }, reject);
        } else {
          resolve(null);
        }
      });
    });

    Promise.allSettled(filePromises)
      .then(results => {
        const successfulUploads = results
          .filter((result): result is PromiseFulfilledResult<FileUploadItem | null> => result.status === "fulfilled")
          .map(result => result.value)
          .filter((item): item is FileUploadItem => item !== null);

        if (successfulUploads.length > 0) {
          successfulUploads.forEach(fileUploadItem => {
            this.addFileToNewUploadsFileTree(fileUploadItem.name, fileUploadItem);
          });
          this.showFileUploadBanner("success", `${successfulUploads.length} files uploaded successfully!`);
        }

        const failedCount = results.length - successfulUploads.length;
        if (failedCount > 0) {
          this.showFileUploadBanner("error", `${failedCount} files failed to upload.`);
        }

        this.uploadedFiles.emit(Array.from(this.newUploadNodeToFileItems.values()));
      })
      .catch(error => {
        this.showFileUploadBanner("error", `Unexpected error: ${error.message}`);
      });
  }

  onPreviouslyUploadedFileDeleted(node: DatasetVersionFileTreeNode) {
    this.removeFileTreeNode(node, true);
    const paths = getPathsFromTreeNode(node);
    this.removingFilePaths.emit(paths);
  }

  onNewUploadsFileDeleted(node: DatasetVersionFileTreeNode) {
    this.removeFileTreeNode(node, false);
    this.uploadedFiles.emit(Array.from(this.newUploadNodeToFileItems.values()));
  }

  private removeFileTreeNode(node: DatasetVersionFileTreeNode, fromPreviouslyUploads: boolean) {
    if (fromPreviouslyUploads) {
      if (!this.previouslyUploadFilesManager) {
        this.previouslyUploadFilesManager = new DatasetVersionFileTreeManager(this.previouslyUploadFiles);
      }
      if (this.previouslyUploadFilesManager) {
        this.previouslyUploadFilesManager.removeNode(node);
        this.previouslyUploadFiles = [...this.previouslyUploadFilesManager.getRootNodes()];
      }
    } else {
      // from new uploads
      this.newUploadFileTreeManager.removeNode(node);
      this.newUploadFileTreeNodes = [...this.newUploadFileTreeManager.getRootNodes()];
      this.removeNodeAndChildrenFromFileItemsMap(node);
    }
  }

  private removeNodeAndChildrenFromFileItemsMap(node: DatasetVersionFileTreeNode) {
    this.newUploadNodeToFileItems.delete(node);

    // Recursively remove children if it's a directory
    if (node.type === "directory" && node.children) {
      node.children.forEach(child => this.removeNodeAndChildrenFromFileItemsMap(child));
    }
  }

  private addFileToNewUploadsFileTree(path: string, fileUploadItem: FileUploadItem) {
    const newNode = this.newUploadFileTreeManager.addNodeWithPath(path);

    this.newUploadFileTreeNodes = [...this.newUploadFileTreeManager.getRootNodes()];
    this.newUploadNodeToFileItems.set(newNode, fileUploadItem);
  }
}
