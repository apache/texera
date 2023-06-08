import { Component, OnInit } from "@angular/core";
import { UserProjectService } from "../../service/user-project/user-project.service";
import { DashboardProject } from "../../type/dashboard-project.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "../../../../common/service/notification/notification.service";

@UntilDestroy()
@Component({
  selector: "texera-user-project-list",
  templateUrl: "./user-project.component.html",
  styleUrls: ["./user-project.component.scss"],
})
export class UserProjectComponent implements OnInit {
  // store list of projects / variables to create and edit projects
  public userProjectEntries: DashboardProject[] = [];
  public userProjectEntriesIsEditingName: number[] = [];
  public userProjectEntriesIsEditingDescription: number[] = [];
  public collapsedProjectDescriptions: number[] = [];
  public createButtonIsClicked: boolean = false;
  public createProjectName: string = "";

  // used to manage setting project colors
  public userProjectToColorInputIndexMap: Map<number, number> = new Map(); // maps each project to its color wheel input index, even after reordering / sorting of projects
  public userProjectInputColors: string[] = []; // stores the color wheel input for each project, each color string must start with '#'
  public colorBrightnessMap: Map<number, boolean> = new Map(); // tracks brightness of each project's color, to make sure info remains visible against white background
  public colorInputToggleArray: boolean[] = []; // tracks which project's color wheel is toggled on or off

  constructor(private userProjectService: UserProjectService, private notificationService: NotificationService) {}

  ngOnInit(): void {
    this.getUserProjectArray();
  }

  public deleteProject(pid: number): void {
    if (pid == undefined) {
      throw new Error("pid is undefined in deleteProject().");
    }
    this.userProjectService
      .deleteProject(pid)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.getUserProjectArray());
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
        .subscribe({
          next: createdProject => {
            this.userProjectEntries.push(createdProject); // update local list of projects

            // add color wheel input record for the newly created, colorless project
            this.userProjectToColorInputIndexMap.set(createdProject.pid, this.userProjectEntries.length - 1);
            this.userProjectInputColors.push("#ffffff");
            this.colorInputToggleArray.push(false);

            this.unclickCreateButton();
          },
          error: (err: unknown) => {
            // @ts-ignore
            this.notificationService.error(err.error.message);
          },
        });
    } else {
      // show error message and don't call backend
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

  private getUserProjectArray() {
    this.userProjectService.refreshProjectList();
    this.userProjectService
      .retrieveProjectList()
      .pipe(untilDestroyed(this))
      .subscribe(projectEntries => {
        this.userProjectEntries = projectEntries;
      });
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
}
