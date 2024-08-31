import { Component, OnInit } from "@angular/core";
import { FormBuilder, FormGroup, Validators } from "@angular/forms";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { UserService } from "../../../../common/service/user/user.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { environment } from "../../../../../environments/environment";

@UntilDestroy()
@Component({
  selector: "texera-settings",
  templateUrl: "./settings.component.html",
  styleUrls: ["./settings.component.scss"],
})
export class SettingsComponent implements OnInit {
  settingsForm!: FormGroup;
  currentDataTransferBatchSize!: number;
  isSaving: boolean = false;

  constructor(
    private fb: FormBuilder,
    private workflowActionService: WorkflowActionService,
    private workflowPersistService: WorkflowPersistService,
    private userService: UserService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.currentDataTransferBatchSize =
      this.workflowActionService.getWorkflowContent().settings.dataTransferBatchSize ||
      environment.defaultDataTransferBatchSize;

    this.settingsForm = this.fb.group({
      dataTransferBatchSize: [this.currentDataTransferBatchSize, [Validators.required, Validators.min(1)]],
    });

    this.settingsForm.valueChanges.pipe(untilDestroyed(this)).subscribe(value => {
      if (this.settingsForm.valid) {
        this.confirmUpdateDataTransferBatchSize(value.dataTransferBatchSize);
      }
    });

    this.workflowActionService
      .workflowChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.currentDataTransferBatchSize =
          this.workflowActionService.getWorkflowContent().settings.dataTransferBatchSize ||
          environment.defaultDataTransferBatchSize;
        this.settingsForm.patchValue(
          { dataTransferBatchSize: this.currentDataTransferBatchSize },
          { emitEvent: false }
        );
      });
  }

  public confirmUpdateDataTransferBatchSize(dataTransferBatchSize: number): void {
    if (dataTransferBatchSize > 0) {
      this.workflowActionService.setWorkflowDataTransferBatchSize(dataTransferBatchSize);
      if (this.userService.isLogin()) {
        this.persistWorkflow();
      }
    }
  }

  public persistWorkflow(): void {
    this.isSaving = true;
    this.workflowPersistService
      .persistWorkflow(this.workflowActionService.getWorkflow())
      .pipe(untilDestroyed(this))
      .subscribe({
        error: (e: unknown) => this.notificationService.error((e as Error).message),
      })
      .add(() => (this.isSaving = false));
  }
}
