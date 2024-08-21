import { Component, OnInit } from "@angular/core";
import { FormBuilder, FormGroup, Validators } from "@angular/forms";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { UserService } from "../../../../common/service/user/user.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";

@UntilDestroy()
@Component({
  selector: "texera-settings",
  templateUrl: "./settings.component.html",
  styleUrls: ["./settings.component.scss"],
})
export class SettingsComponent implements OnInit {
  settingsForm!: FormGroup;
  currentBatchSize!: number;
  isSaving: boolean = false;

  constructor(
    private fb: FormBuilder,
    private workflowActionService: WorkflowActionService,
    private workflowPersistService: WorkflowPersistService,
    private userService: UserService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.currentBatchSize = this.workflowActionService.getWorkflowContent().settings.batchSize || 400;

    this.settingsForm = this.fb.group({
      packetSize: [this.currentBatchSize, [Validators.required, Validators.min(1)]],
    });

    this.settingsForm.valueChanges
      .pipe(untilDestroyed(this))
      .subscribe(value => {
        if (this.settingsForm.valid) {
          this.confirmUpdateBatchSize(value.packetSize);
        }
      });

    this.workflowActionService
      .workflowChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.currentBatchSize = this.workflowActionService.getWorkflowContent().settings.batchSize || 400;
        this.settingsForm.patchValue({ packetSize: this.currentBatchSize }, { emitEvent: false });
      });
  }

  public confirmUpdateBatchSize(batchSize: number): void {
    if (batchSize > 0) {
      this.workflowActionService.setWorkflowBatchSize(batchSize);
      if (this.userService.isLogin()) {
        this.persistWorkflow();
      }
    }
  }

  public persistWorkflow(): void {
    this.isSaving = true;
    this.workflowPersistService
        .persistWorkflow(this.workflowActionService.getWorkflow())
         .pipe(
           untilDestroyed(this)
         )
         .subscribe({
           error: (e: unknown) => this.notificationService.error((e as Error).message),
         })
         .add(() => (this.isSaving = false));
  }
}
