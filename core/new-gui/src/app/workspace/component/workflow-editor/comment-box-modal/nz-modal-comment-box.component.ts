import { Component, HostListener, Inject, Input, LOCALE_ID } from "@angular/core";
import { NzModalRef } from "ng-zorro-antd/modal";
import { CommentBox, Comment } from "src/app/workspace/types/workflow-common.interface";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import { UserService } from "src/app/common/service/user/user.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { User } from "src/app/common/type/user";
import { untilDestroyed } from "@ngneat/until-destroy";
import { UntilDestroy } from "@ngneat/until-destroy";
import { formatDate } from "@angular/common";
import { Array as YArray } from "yjs";
import { YType } from "../../../types/shared-editing.interface";

@UntilDestroy()
@Component({
  selector: "texera-nz-modal-comment-box",
  templateUrl: "./nz-modal-comment-box.component.html",
  styleUrls: ["./nz-modal-comment-box.component.scss"],
})
export class NzModalCommentBoxComponent {
  @Input() commentBox!: YType<CommentBox>;
  public user?: User;

  constructor(
    @Inject(LOCALE_ID) public locale: string,
    public workflowActionService: WorkflowActionService,
    public userService: UserService,
    public modal: NzModalRef<any, number>,
    public notificationService: NotificationService
  ) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(user => (this.user = user));
  }

  inputValue = "";
  submitting = false;
  editValue = "";

  public onClickAddComment(): void {
    this.submitting = true;
    this.addComment(this.inputValue);
    this.inputValue = "";
    this.submitting = false;
  }

  public addComment(content: string): void {
    if (!this.user) {
      return;
    }
    // A compromise: we create the timestamp in the frontend since the entire comment is managed together, in JSON format
    const creationTime: string = new Date().toISOString();
    const creatorName = this.user.name;
    const creatorID = this.user.uid;
    this.workflowActionService.addComment(
      { content, creatorName, creatorID, creationTime },
      this.commentBox.get("commentBoxID").toJSON() as string
    );
  }

  public deleteComment(creatorID: number, creationTime: string): void {
    if (!this.user) {
      return;
    }
    this.workflowActionService.deleteComment(
      creatorID,
      creationTime,
      this.commentBox.get("commentBoxID").toJSON() as string
    );
  }

  public toggleEditInput(creatorName: string, creationTime: string): void {
    const currTxArea = document.getElementById("txarea" + creatorName + creationTime);
    const currComment = document.getElementById("comment" + creatorName + creationTime);
    const btn = document.getElementById("editbtn" + creatorName + creationTime);
    if (currTxArea == null || btn == null || currComment == null) {
      return;
    }
    const hiddenTextArea = currTxArea.getAttribute("hidden");
    const hiddenComment = currComment.getAttribute("hidden");
    if (hiddenTextArea && !hiddenComment) {
      currComment.setAttribute("hidden", "hidden");
      currTxArea.removeAttribute("hidden");
      btn.removeAttribute("hidden");
      if (currComment.textContent != null) {
        this.editValue = currComment.textContent;
      }
    } else {
      currTxArea.setAttribute("hidden", "hidden");
      btn.setAttribute("hidden", "hidden");
      currComment.removeAttribute("hidden");
      this.editValue = "";
    }
  }
  public editComment(creatorID: number, creatorName: string, creationTime: string): void {
    if (!this.user) {
      return;
    }
    const newContent = this.editValue;
    this.editValue = "";
    this.workflowActionService.editComment(
      creatorID,
      creationTime,
      this.commentBox.get("commentBoxID").toJSON() as string,
      newContent
    );
    const currTxArea = document.getElementById("txarea" + creatorName + creationTime);
    const btn = document.getElementById("editbtn" + creatorName + creationTime);
    if (currTxArea == null || btn == null) {
      return;
    }
    currTxArea.setAttribute("hidden", "hidden");
    btn.setAttribute("hidden", "hidden");
  }
  public replyToComment(creatorName: string, content: string) {
    this.inputValue += "@" + creatorName + ":\"" + content + "\"\n";
  }
  toRelative(datetime: string): string {
    return formatDate(new Date(datetime), "MM/dd/yyyy, hh:mm:ss a z", this.locale);
  }

  @HostListener("window:keydown", ["$event"])
  onKeyDown(event: KeyboardEvent) {
    if ((event.metaKey || event.ctrlKey) && event.key == "Enter") {
      this.onClickAddComment();
      event.preventDefault();
    }
  }
}
