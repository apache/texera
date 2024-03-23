import { Component, OnInit } from "@angular/core";
import { UserProjectService } from "../../service/user-project/user-project.service";
import { DashboardProject } from "../../type/dashboard-project.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { UserService } from "../../../../common/service/user/user.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { PublicProjectComponent } from "./public-project/public-project.component";

@UntilDestroy()
@Component({
  selector: "texera-user-project-list",
  templateUrl: "./user-project.component.html",
  styleUrls: ["./user-project.component.scss"],
})
export class UserProjectComponent implements OnInit {
  // store list of projects / variables to create and edit projects
  public userProjectEntries: DashboardProject[] = [];
  public createButtonIsClicked: boolean = false;
  public createProjectName: string = "";
  public uid: number | undefined;

  constructor(
    private userProjectService: UserProjectService,
    private notificationService: NotificationService,
    private userService: UserService,
    private modalService: NzModalService
  ) {
    this.uid = this.userService.getCurrentUser()?.uid;
  }

  ngOnInit(): void {
    this.userProjectService
      .getProjectList()
      .pipe(untilDestroyed(this))
      .subscribe(projectEntries => {
        this.userProjectEntries = projectEntries;
      });
  }

  public deleteProject(pid: number): void {
    this.userProjectService
      .deleteProject(pid)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.ngOnInit());
  }

  public clickCreateButton(): void {
    this.createButtonIsClicked = true;
  }

  public unclickCreateButton(): void {
    this.createButtonIsClicked = false;
    this.createProjectName = "";
  }

  public createNewProject(): void {
    if (this.isValidNewProjectName(this.createProjectName)) {
      this.userProjectService
        .createProject(this.createProjectName)
        .pipe(untilDestroyed(this))
        .subscribe(() => this.ngOnInit());
    } else {
      this.notificationService.error(
        `Cannot create project named: "${this.createProjectName}".  It must be a non-empty, unique name`
      );
    }
  }

  public sortByCreationTime(): void {
    this.userProjectEntries.sort((p1, p2) =>
      p1.creationTime !== undefined && p2.creationTime !== undefined ? p1.creationTime - p2.creationTime : 0
    );
  }

  public sortByNameAsc(): void {
    this.userProjectEntries.sort((p1, p2) => p1.name.toLowerCase().localeCompare(p2.name.toLowerCase()));
  }

  public sortByNameDesc(): void {
    this.userProjectEntries.sort((p1, p2) => p2.name.toLowerCase().localeCompare(p1.name.toLowerCase()));
  }

  private isValidNewProjectName(newName: string, oldProject?: DashboardProject): boolean {
    if (typeof oldProject === "undefined") {
      return newName.length != 0 && this.userProjectEntries.filter(project => project.name === newName).length === 0;
    } else {
      return (
        newName.length != 0 &&
        this.userProjectEntries.filter(project => project.pid !== oldProject.pid && project.name === newName).length ===
          0
      );
    }
  }

  public openPublicProject(): void {
    const modalRef = this.modalService.create({
      nzContent: PublicProjectComponent,
      nzData: { disabledList: new Set(this.userProjectEntries.map(project => project.pid)) },
      nzFooter: null,
      nzTitle: "Add Public Projects",
      nzCentered: true,
    });
    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe(() => this.ngOnInit());
  }
}
