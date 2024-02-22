import { Component, EventEmitter, Input, OnInit, Output } from "@angular/core";
import { DashboardProject } from "../../../type/dashboard-project.interface";
import { UserProjectService } from "../../../service/user-project/user-project.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ShareAccessComponent } from "../../share-access/share-access.component";
import { NzModalService } from "ng-zorro-antd/modal";
import { PublicProjectService } from "../../../service/public-project/public-project.service";
import { UserService } from "../../../../../common/service/user/user.service";

@UntilDestroy()
@Component({
  selector: "texera-user-project-list-item",
  templateUrl: "./user-project-list-item.component.html",
  styleUrls: ["./user-project-list-item.component.scss"],
})
export class UserProjectListItemComponent implements OnInit {
  public readonly ROUTER_USER_PROJECT_BASE_URL = "/dashboard/user-project";
  public readonly MAX_PROJECT_DESCRIPTION_CHAR_COUNT = 10000;
  private _entry?: DashboardProject;
  @Input() public keywords: string[] = [];
  @Input()
  get entry(): DashboardProject {
    if (!this._entry) {
      throw new Error("entry property must be provided to UserProjectListItemComponent.");
    }
    return this._entry;
  }
  set entry(value: DashboardProject) {
    this._entry = value;
  }
  @Output() deleted = new EventEmitter<void>();
  @Output() refresh = new EventEmitter<void>();
  @Input() editable = false;
  @Input() uid: number | undefined;
  editingColor = false;
  editingName = false;
  editingDescription = false;
  descriptionCollapsed = true;
  color = "#ffffff";
  isAdmin: boolean = false;
  isPublic: boolean = false;
  /** To make sure info remains visible against white background */
  get lightColor() {
    return UserProjectService.isLightColor(this.color);
  }

  constructor(
    private userProjectService: UserProjectService,
    private notificationService: NotificationService,
    private modalService: NzModalService,
    private userService: UserService,
    private publicProjectService: PublicProjectService
  ) {
    this.isAdmin = this.userService.isAdmin();
  }

  ngOnInit(): void {
    if (this.entry.color) {
      this.color = this.entry.color;
    }
    this.publicProjectService
      .getType(this.entry.pid)
      .pipe(untilDestroyed(this))
      .subscribe(type => {
        this.isPublic = type === "Public";
      });
  }

  updateProjectColor(): void {
    if (!this.editable) {
      return;
    }
    const color = this.color.substring(1);
    this.editingColor = false;
    // validate that color is in proper HEX format
    if (UserProjectService.isInvalidColorFormat(color)) {
      this.notificationService.error(
        `Cannot update color for project: "${this.entry.name}".  It must be a valid HEX color format`
      );
      return;
    }

    if (color === this.entry.color) {
      return;
    }

    this.userProjectService
      .updateProjectColor(this.entry.pid, color)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.color = color;
        this.entry = { ...this.entry, color: color };
      });
  }

  removeProjectColor(): void {
    this.editingColor = false;

    this.userProjectService
      .deleteProjectColor(this.entry.pid)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.color = "#ffffff"; // reset color wheel
        this.entry = { ...this.entry, color: null };
      });
  }

  saveProjectName(name: string): void {
    // nothing happens if name is the same
    if (this.entry.name === name) {
      this.editingName = false;
    } else {
      this.userProjectService
        .updateProjectName(this.entry.pid, name)
        .pipe(untilDestroyed(this))
        .subscribe(() => {
          if (!this.entry) {
            throw new Error("entry property must be provided to UserProjectListItemComponent.");
          }
          this.editingName = false;
          this.entry.name = name;
        });
    }
  }

  saveProjectDescription(description: string): void {
    // nothing happens if description is the same
    if (this.entry.description === description) {
      this.editingDescription = false;
      return;
    }

    // update the project's description
    this.userProjectService
      .updateProjectDescription(this.entry.pid, description)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.entry.description = description;
        this.notificationService.success(`Saved description for project: "${this.entry.name}".`);
        this.editingDescription = false;
      });
  }

  public onClickOpenShareAccess(): void {
    const modalRef = this.modalService.create({
      nzContent: ShareAccessComponent,
      nzComponentParams: {
        writeAccess: this.entry.accessLevel === "WRITE",
        type: "project",
        id: this.entry.pid,
      },
      nzFooter: null,
      nzTitle: "Share this project with others",
      nzCentered: true,
    });
    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe(() => this.refresh.emit());
  }
  public visibilityChange(): void {
    if (this.isPublic) {
      this.publicProjectService
        .makePrivate(this.entry.pid)
        .pipe(untilDestroyed(this))
        .subscribe(() => this.ngOnInit());
    } else {
      this.publicProjectService
        .makePublic(this.entry.pid)
        .pipe(untilDestroyed(this))
        .subscribe(() => this.ngOnInit());
    }
  }
}
