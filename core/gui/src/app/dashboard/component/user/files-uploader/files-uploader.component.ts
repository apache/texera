import { Component, EventEmitter, Input, Output } from "@angular/core";
import { FileUploadItem } from "../../../type/dashboard-file.interface";
import { NgxFileDropEntry } from "ngx-file-drop";
import {
  DatasetVersionFileTreeManager,
  DatasetFileNode,
  getPathsUnderOrEqualDatasetFileNode,
} from "../../../../common/type/datasetVersionFileTree";
import { environment } from "../../../../../environments/environment";
import { NotificationService } from "../../../../common/service/notification/notification.service";

@Component({
  selector: "texera-user-files-uploader",
  templateUrl: "./files-uploader.component.html",
  styleUrls: ["./files-uploader.component.scss"],
})
export class FilesUploaderComponent {
  @Input()
  previouslyUploadFiles: DatasetFileNode[] | undefined;
  previouslyUploadFilesManager: DatasetVersionFileTreeManager | undefined;

  @Output()
  uploadedFiles = new EventEmitter<FileUploadItem[]>();
  //
  @Output()
  removingFilePaths = new EventEmitter<string[]>();

  newUploadNodeToFileItems: Map<DatasetFileNode, FileUploadItem> = new Map<DatasetFileNode, FileUploadItem>();
  newUploadFileTreeManager: DatasetVersionFileTreeManager = new DatasetVersionFileTreeManager();
  newUploadFileTreeNodes: DatasetFileNode[] = [];

  fileUploadingFinished: boolean = false;
  // four types: "success", "info", "warning" and "error"
  fileUploadBannerType: "error" | "success" | "info" | "warning" = "success";
  fileUploadBannerMessage: string = "";

  constructor(private notificationService: NotificationService) {}

  hideBanner() {
    this.fileUploadingFinished = false;
  }

  showFileUploadBanner(bannerType: "error" | "success" | "info" | "warning", bannerMessage: string) {
    this.fileUploadingFinished = true;
    this.fileUploadBannerType = bannerType;
    this.fileUploadBannerMessage = bannerMessage;
  }

  public fileDropped(files: NgxFileDropEntry[]) {
    // this promise create the FileEntry from each of the NgxFileDropEntry
    // this filePromises is used to ensure the atomicity of file upload
    const filePromises = files.map(droppedFile => {
      return new Promise<FileUploadItem | null>((resolve, reject) => {
        if (droppedFile.fileEntry.isFile) {
          const fileEntry = droppedFile.fileEntry as FileSystemFileEntry;
          fileEntry.file(file => {
            // Check the file size here
            if (file.size > environment.singleFileUploadMaximumSizeMB * 1024 * 1024) {
              // If the file is too large, reject the promise
              this.notificationService.error(
                `File ${file.name}'s size exceeds the maximum limit of ${environment.singleFileUploadMaximumSizeMB}MB.`
              );
              reject(null);
            } else {
              // If the file size is within the limit, proceed
              resolve({
                file: file,
                name: droppedFile.relativePath,
                description: "",
                uploadProgress: 0,
                isUploadingFlag: false,
              });
            }
          }, reject);
        } else {
          resolve(null);
        }
      });
    });

    Promise.allSettled(filePromises)
      .then(results => {
        // once all FileUploadItems are created/some of them are rejected, enter this block
        const successfulUploads = results
          .filter((result): result is PromiseFulfilledResult<FileUploadItem | null> => result.status === "fulfilled")
          .map(result => result.value)
          .filter((item): item is FileUploadItem => item !== null);

        if (successfulUploads.length > 0) {
          successfulUploads.forEach(fileUploadItem => {
            this.addFileToNewUploadsFileTree(fileUploadItem.name, fileUploadItem);
          });
          const successMessage = `${successfulUploads.length} file${successfulUploads.length > 1 ? "s" : ""} selected successfully!`;
          this.showFileUploadBanner("success", successMessage);
        }

        const failedCount = results.length - successfulUploads.length;
        if (failedCount > 0) {
          const errorMessage = `${failedCount} file${failedCount > 1 ? "s" : ""} failed to be selected.`;
          this.showFileUploadBanner("error", errorMessage);
        }

        this.uploadedFiles.emit(Array.from(this.newUploadNodeToFileItems.values()));
      })
      .catch(error => {
        this.showFileUploadBanner("error", `Unexpected error: ${error.message}`);
      });
  }

  onPreviouslyUploadedFileDeleted(node: DatasetFileNode) {
    this.removeFileTreeNode(node, true);
    const paths = getPathsUnderOrEqualDatasetFileNode(node);
    this.removingFilePaths.emit(paths);
  }

  onNewUploadsFileDeleted(node: DatasetFileNode) {
    this.removeFileTreeNode(node, false);
    this.uploadedFiles.emit(Array.from(this.newUploadNodeToFileItems.values()));
  }

  private removeFileTreeNode(node: DatasetFileNode, fromPreviouslyUploads: boolean) {
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

  private removeNodeAndChildrenFromFileItemsMap(node: DatasetFileNode) {
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
