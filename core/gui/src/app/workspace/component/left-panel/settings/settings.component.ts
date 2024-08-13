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
    private workflowPersistService: WorkflowPersistService,  // 注入服务
    private userService: UserService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    console.log("------")
    console.log(this.workflowActionService.getWorkflowContent().batchSize)
    console.log("------")
    // this.currentBatchSize = this.workflowActionService.getWorkflowContent().batchSize || 400; // 默认值为 400

    this.settingsForm = this.fb.group({
      packetSize: [this.currentBatchSize, [Validators.required, Validators.min(1)]],
    });

    this.workflowActionService
      .workflowChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.currentBatchSize = this.workflowActionService.getWorkflowContent().batchSize || 400;
        this.settingsForm.patchValue({ packetSize: this.currentBatchSize });
      });

  }

  onSubmit(): void {
    if (this.settingsForm.valid) {
      const packetSize = this.settingsForm.value.packetSize;
      this.confirmUpdateBatchSize(packetSize);
    }
  }

  public confirmUpdateBatchSize(batchSize: number): void {
    if (batchSize > 0) {
      // 更新 batchSize
      this.workflowActionService.setWorkflowBatchSize(batchSize);

      // 如果用户已登录，则保存工作流
      if (this.userService.isLogin()) {
        this.persistWorkflow();
      }
    }
  }

  public persistWorkflow(): void {
    this.isSaving = true;

    // 调用 persistWorkflow 方法保存当前的工作流状态
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
