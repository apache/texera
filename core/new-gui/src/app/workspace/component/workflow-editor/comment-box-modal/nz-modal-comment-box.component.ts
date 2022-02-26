import { Component, Input, OnInit } from "@angular/core";
import { FormBuilder, Validators } from "@angular/forms";
import { NzModalRef } from "ng-zorro-antd/modal";
import { CommentBox } from "src/app/workspace/types/workflow-common.interface";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import { UserService } from "src/app/common/service/user/user.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { User } from "src/app/common/type/user";
import { untilDestroyed } from "@ngneat/until-destroy";
import { UntilDestroy } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-ngbd-modal-comment-box",
  templateUrl: "./nz-modal-comment-box.component.html",
  styleUrls: ["./nz-modal-comment-box.component.scss"],
})
export class NzModalCommentBoxComponent {
  @Input() commentBox!: CommentBox;

  public user: User | undefined;

  public commentForm = this.formBuilder.group({
    comment: ["", [Validators.required]],
  });
  constructor(
    public workflowActionService: WorkflowActionService,
    private formBuilder: FormBuilder,
    public userService: UserService,
    public modal: NzModalRef<any, number>,
    public notificationService: NotificationService
  ) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(user => (this.user = user));
  }

  public onClickAddComment(): void {
    if (this.commentForm.get("comment")?.invalid) {
      this.notificationService.error("Cannot Submit Empty Comment!!");
      return;
    }
    const newComment = this.commentForm.get("comment")?.value;
    this.addComment(newComment);
    this.commentForm.get("comment")?.setValue("");
  }

  public addComment(newComment: string): void {
    const currentTime: string = new Date().toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }) + "(PST)";
    const creator = this.user?.name;
    const creatorID = this.user?.uid;
    this.workflowActionService.addComment(
      { content: newComment, creationTime: currentTime, creator: creator, creatorID: creatorID },
      this.commentBox.commentBoxID
    );
  }
}
