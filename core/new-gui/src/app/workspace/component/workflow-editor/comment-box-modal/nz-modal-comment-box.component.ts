import {AfterViewInit, Component, Inject, Input, LOCALE_ID} from "@angular/core";
import {NzModalRef} from "ng-zorro-antd/modal";
import {CommentBox} from "src/app/workspace/types/workflow-common.interface";
import {WorkflowActionService} from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import {UserService} from "src/app/common/service/user/user.service";
import {NotificationService} from "../../../../common/service/notification/notification.service";
import {User} from "src/app/common/type/user";
import {untilDestroyed} from "@ngneat/until-destroy";
import {UntilDestroy} from "@ngneat/until-destroy";
import {formatDate} from "@angular/common";

@UntilDestroy()
@Component({
  selector: "texera-nz-modal-comment-box",
  templateUrl: "./nz-modal-comment-box.component.html",
  styleUrls: ["./nz-modal-comment-box.component.scss"],
})
export class NzModalCommentBoxComponent implements AfterViewInit {
  @Input() commentBox!: CommentBox;
  public user?: User;

  constructor(
    @Inject(LOCALE_ID) public locale: string,
    public workflowActionService: WorkflowActionService,
    public userService: UserService,
    public modal: NzModalRef<any, number>,
    public notificationService: NotificationService
  ) {
  }

  ngAfterViewInit(): void {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(user => (this.user = user));
  }

  inputValue = "";
  submitting = false;

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
      {content, creatorName: creatorName, creatorID, creationTime},
      this.commentBox.commentBoxID
    );
  }

  toRelative(datetime: string): string {
    return formatDate(new Date(datetime), "MM/dd/yyyy, hh:mm:ss a z", this.locale);
  }
}
