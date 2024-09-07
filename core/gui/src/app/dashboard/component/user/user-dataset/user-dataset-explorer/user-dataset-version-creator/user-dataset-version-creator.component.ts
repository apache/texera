import { Component, EventEmitter, Input, OnInit, Output } from "@angular/core";
import { FormBuilder, FormGroup, Validators } from "@angular/forms";
import { FormlyFieldConfig } from "@ngx-formly/core";
import { DatasetService } from "../../../../../service/user/dataset/dataset.service";
import { FileUploadItem } from "../../../../../type/dashboard-file.interface";
import { Dataset, DatasetVersion } from "../../../../../../common/type/dataset";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "../../../../../../common/service/notification/notification.service";
import sanitize from "sanitize-filename";
import { HttpErrorResponse } from "@angular/common/http";

@UntilDestroy()
@Component({
  selector: "texera-user-dataset-version-creator",
  templateUrl: "./user-dataset-version-creator.component.html",
  styleUrls: ["./user-dataset-version-creator.component.scss"],
})
export class UserDatasetVersionCreatorComponent implements OnInit {
  @Input()
  isCreatingVersion: boolean = false;

  @Input()
  baseVersion: DatasetVersion | undefined;

  // this emits the ID of the newly created version/dataset, will emit 0 if creation is failed.
  @Output()
  datasetOrVersionCreationID: EventEmitter<number> = new EventEmitter<number>();

  isCreateButtonDisabled: boolean = false;

  newUploadFiles: FileUploadItem[] = [];

  removedFilePaths: string[] = [];

  public form: FormGroup = new FormGroup({});
  model: any = {};
  fields: FormlyFieldConfig[] = [];
  isDatasetPublic: boolean = false;

  // used when creating the dataset
  isDatasetNameSanitized: boolean = false;

  // boolean to control if is uploading
  isUploading: boolean = false;

  constructor(
    private datasetService: DatasetService,
    private notificationService: NotificationService,
    private formBuilder: FormBuilder
  ) {}

  ngOnInit() {
    this.setFormFields();
    this.isDatasetNameSanitized = false;
  }

  private setFormFields() {
    this.fields = this.isCreatingVersion
      ? [
          // Fields when isCreatingVersion is true
          {
            key: "versionDescription",
            type: "input",
            defaultValue: "",
            templateOptions: {
              label: "Describe the new version",
              required: false,
            },
          },
        ]
      : [
          // Fields when isCreatingVersion is false
          {
            key: "name",
            type: "input",
            templateOptions: {
              label: "Name",
              required: true,
            },
          },
          {
            key: "description",
            type: "input",
            defaultValue: "",
            templateOptions: {
              label: "Description",
            },
          },
          {
            key: "versionDescription",
            type: "input",
            defaultValue: "",
            templateOptions: {
              label: "Version Description",
              required: false,
            },
          },
        ];
  }
  get formControlNames(): string[] {
    return Object.keys(this.form.controls);
  }

  datasetNameSanitization(datasetName: string): string {
    const sanitizedDatasetName = sanitize(datasetName);
    if (sanitizedDatasetName != datasetName) {
      this.isDatasetNameSanitized = true;
    }
    return sanitizedDatasetName;
  }

  private triggerValidation() {
    Object.keys(this.form.controls).forEach(field => {
      const control = this.form.get(field);
      control?.markAsTouched({ onlySelf: true });
    });
  }

  onClickCancel() {
    this.datasetOrVersionCreationID.emit(0);
  }

  onClickCreate() {
    // check if the form is valid
    this.triggerValidation();

    if (!this.form.valid) {
      return; // Stop further execution if the form is not valid
    }

    if (this.newUploadFiles.length == 0 && this.removedFilePaths.length == 0) {
      this.notificationService.error(
        `Please either upload new file(s) or remove old file(s) when creating a new ${this.isCreatingVersion ? "Version" : "Dataset"}`
      );
      return;
    }

    this.isUploading = true;
    if (this.isCreatingVersion && this.baseVersion) {
      const versionName = this.form.get("versionDescription")?.value;
      this.datasetService
        .createDatasetVersion(this.baseVersion?.did, versionName, this.removedFilePaths, this.newUploadFiles)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: res => {
            this.notificationService.success("Version Created");
            this.datasetOrVersionCreationID.emit(res.dvid);
            this.isUploading = false;
          },
          error: (res: unknown) => {
            const err = res as HttpErrorResponse;
            this.notificationService.error(`Version creation failed: ${err.error.message}`);
            this.isUploading = false;
          },
        });
    } else {
      const ds: Dataset = {
        name: this.datasetNameSanitization(this.form.get("name")?.value),
        description: this.form.get("description")?.value,
        isPublic: this.isDatasetPublic ? 1 : 0,
        did: undefined,
        ownerUid: undefined,
        storagePath: undefined,
        creationTime: undefined,
        versionHierarchy: undefined,
      };
      const initialVersionName = this.form.get("versionDescription")?.value;

      // do the name sanitization

      this.datasetService
        .createDataset(ds, initialVersionName, this.newUploadFiles)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: res => {
            this.notificationService.success(
              `Dataset '${ds.name}' Created. ${this.isDatasetNameSanitized ? "We have sanitized your provided dataset name for the compatibility reason" : ""}`
            );
            this.datasetOrVersionCreationID.emit(res.dataset.did);
            this.isUploading = false;
          },
          error: (res: unknown) => {
            const err = res as HttpErrorResponse;
            this.notificationService.error(`Dataset ${ds.name} creation failed: ${err.error.message}`);
            this.isUploading = false;
          },
        });
    }
  }

  onPublicStatusChange(newValue: boolean): void {
    // Handle the change in dataset public status
    this.isDatasetPublic = newValue;
  }

  onNewUploadFilesChanged(files: FileUploadItem[]) {
    this.newUploadFiles = files;
  }

  onRemovingFilePathsChanged(paths: string[]) {
    this.removedFilePaths = this.removedFilePaths.concat(paths);
  }
}
