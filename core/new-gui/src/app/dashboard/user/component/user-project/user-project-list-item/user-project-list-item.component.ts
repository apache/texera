import { Component, EventEmitter, Input, OnInit, Output } from "@angular/core";
import { UserProject } from "../../../type/user-project";
import { UserProjectService } from "../../../service/user-project/user-project.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-user-project-list-item",
  templateUrl: "./user-project-list-item.component.html",
  styleUrls: ["./user-project-list-item.component.css"],
})
export class UserProjectListItemComponent implements OnInit {
  public readonly ROUTER_USER_PROJECT_BASE_URL = "/dashboard/user-project";
  public readonly MAX_PROJECT_DESCRIPTION_CHAR_COUNT = 10000;
  private _entry?: UserProject;
  @Input()
  get entry(): UserProject {
    if (!this._entry) {
      throw new Error("entry property must be provided to UserProjectListItemComponent.");
    }
    return this._entry;
  }
  set entry(value: UserProject) {
    this._entry = value;
  }
  /**
   * Whether edit is enabled globally. It is possible to only edit this entry by setting
   * this.editingName = true or this.editingDescription = true.
   */
  @Input() editing = false;
  @Output() deleted = new EventEmitter<void>();

  editingColor = false;
  editingName = false;
  editingDescription = false;
  descriptionCollapsed = false;
  color = "#ffffff";
  /** To make sure info remains visible against white background */
  get lightColor() {
    return UserProjectService.isLightColor(this.color);
  }

  constructor(private userProjectService: UserProjectService, private notificationService: NotificationService) {}

  ngOnInit(): void {
    if (this.entry.color) {
      this.color = this.entry.color;
    }
  }

  updateProjectColor(): void {
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
      .subscribe({
        next: () => {
          this.color = color;
          this.entry = { ...this.entry, color: color };
        },
        error: (err: unknown) => {
          // @ts-ignore
          this.notificationService.error(err.error.message);
        },
      });
  }

  removeProjectColor(): void {
    this.editingColor = false;

    this.userProjectService
      .deleteProjectColor(this.entry.pid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: _ => {
          this.color = "#ffffff"; // reset color wheel
          this.entry = { ...this.entry, color: null };
        },
        error: (err: unknown) => {
          // @ts-ignore
          this.notificationService.error(err.error.message);
        },
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
        .subscribe({
          next: () => {
            if (!this.entry) {
              throw new Error("entry property must be provided to UserProjectListItemComponent.");
            }
            this.editingName = false;
            this.entry.name = name;
          },
          error: (err: unknown) => {
            // @ts-ignore
            this.notificationService.error(err.error.message);
          },
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
      .subscribe({
        next: () => {
          this.entry.description = description;
          this.notificationService.success(`Saved description for project: "${this.entry.name}".`);
        },
        error: (err: unknown) => {
          // @ts-ignore
          this.notificationService.error(err.error.message);
        },
      })
      .add(() => {
        this.editingDescription = false;
      });
  }
}
