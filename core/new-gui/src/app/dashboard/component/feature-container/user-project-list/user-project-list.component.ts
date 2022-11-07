import { Component, OnInit } from "@angular/core";
import { UserProjectService } from "../../../service/user-project/user-project.service";
import { UserProject } from "../../../type/user-project";
import { Router } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { DeletePromptComponent } from "../../delete-prompt/delete-prompt.component";
import { from } from "rxjs";

@UntilDestroy()
@Component({
  selector: "texera-user-project-list",
  templateUrl: "./user-project-list.component.html",
  styleUrls: ["./user-project-list.component.scss"],
})
export class UserProjectListComponent implements OnInit {
  // store list of projects / variables to create and edit projects
  public userProjectEntries: UserProject[] = [];
  public userProjectEntriesIsEditingName: number[] = [];
  public createButtonIsClicked: boolean = false;
  public createProjectName: string = "";

  // used to manage setting project colors
  public userProjectToColorInputIndexMap: Map<number, number> = new Map(); // maps each project to its color wheel input index, even after reordering / sorting of projects
  public userProjectInputColors: string[] = []; // stores the color wheel input for each project, each color string must start with '#'
  public colorBrightnessMap: Map<number, boolean> = new Map(); // tracks brightness of each project's color, to make sure info remains visible against white background
  public colorInputToggleArray: boolean[] = []; // tracks which project's color wheel is toggled on or off

  public readonly ROUTER_USER_PROJECT_BASE_URL = "/dashboard/user-project";

  constructor(
    private userProjectService: UserProjectService,
    private router: Router,
    private notificationService: NotificationService,
    private modalService: NgbModal
  ) {}

  ngOnInit(): void {
    this.getUserProjectArray();
  }

  private getUserProjectArray() {
    this.userProjectService
      .retrieveProjectList()
      .pipe(untilDestroyed(this))
      .subscribe(projectEntries => {
        this.userProjectEntries = projectEntries;

        // map each pid to important color information, for access in HTML template
        let index = 0;
        for (var project of projectEntries) {
          // used to store each project's updated color (via color wheel)
          this.userProjectToColorInputIndexMap.set(project.pid, index);
          this.userProjectInputColors.push(project.color == null ? "#ffffff" : "#" + project.color);
          this.colorInputToggleArray.push(false);

          // determine whether each project's color is light or dark
          if (project.color != null) {
            this.colorBrightnessMap.set(project.pid, this.userProjectService.isLightColor(project.color));
          }
          index++;
        }
      });
  }

  public removeEditStatus(pid: number): void {
    this.userProjectEntriesIsEditingName = this.userProjectEntriesIsEditingName.filter(index => index != pid);
  }

  public saveProjectName(project: UserProject, newName: string): void {
    // nothing happens if name is the same
    if (project.name === newName) {
      this.removeEditStatus(project.pid);
    } else if (this.isValidNewProjectName(newName, project)) {
      this.userProjectService
        .updateProjectName(project.pid, newName)
        .pipe(untilDestroyed(this))
        .subscribe(
          () => {
            this.removeEditStatus(project.pid);
            this.getUserProjectArray(); // refresh list of projects, name is read only property so cannot edit
          },
          (err: unknown) => {
            // @ts-ignore
            this.notificationService.error(err.error.message);
          }
        );
    } else {
      // show error message and do not call backend
      this.notificationService.error(`Cannot create project named: "${newName}".  It must be a non-empty, unique name`);
    }
  }

  public deleteProject(pid: number, name: string, index: number): void {
    const modalRef = this.modalService.open(DeletePromptComponent);
    modalRef.componentInstance.deletionType = "project";
    modalRef.componentInstance.deletionName = name;

    from(modalRef.result)
      .pipe(untilDestroyed(this))
      .subscribe((confirmToDelete: boolean) => {
        if (confirmToDelete && pid != undefined) {
          this.userProjectEntries.splice(index, 1); // update local list of projects

          // remove records of this project from color data structures
          if (this.colorBrightnessMap.has(pid)) {
            this.colorBrightnessMap.delete(pid);
          }
          this.userProjectToColorInputIndexMap.delete(pid);
          this.colorInputToggleArray.splice(index, 1);
        }
      });
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
        .subscribe(
          createdProject => {
            this.userProjectEntries.push(createdProject); // update local list of projects

            // add color wheel input record for the newly created, colorless project
            this.userProjectToColorInputIndexMap.set(createdProject.pid, this.userProjectEntries.length - 1);
            this.userProjectInputColors.push("#ffffff");
            this.colorInputToggleArray.push(false);

            this.unclickCreateButton();
          },
          (err: unknown) => {
            // @ts-ignore
            this.notificationService.error(err.error.message);
          }
        );
    } else {
      // show error message and don't call backend
      this.notificationService.error(
        `Cannot create project named: "${this.createProjectName}".  It must be a non-empty, unique name`
      );
    }
  }

  private isValidNewProjectName(newName: string, oldProject?: UserProject): boolean {
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

  public updateProjectColor(dashboardProjectEntry: UserProject, index: number) {
    let color: string =
      this.userProjectInputColors[this.userProjectToColorInputIndexMap.get(dashboardProjectEntry.pid)!].substring(1);
    this.colorInputToggleArray[index] = false;

    // validate that color is in proper HEX format
    if (this.userProjectService.isInvalidColorFormat(color)) {
      this.notificationService.error(
        `Cannot update color for project: "${dashboardProjectEntry.name}".  It must be a valid HEX color format`
      );
      return;
    }

    if (color === this.userProjectEntries[index].color) {
      return;
    }

    this.userProjectService
      .updateProjectColor(dashboardProjectEntry.pid, color)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          // update local cache of project entries
          let updatedDashboardProjectEntry = { ...dashboardProjectEntry };
          updatedDashboardProjectEntry.color = color;
          const newProjectEntries = this.userProjectEntries.slice();
          newProjectEntries[index] = updatedDashboardProjectEntry;
          this.userProjectEntries = newProjectEntries;

          // update color brightness record for this project
          this.colorBrightnessMap.set(dashboardProjectEntry.pid, this.userProjectService.isLightColor(color));
        },
        error: (err: unknown) => {
          // @ts-ignore
          this.notificationService.error(err.error.message);
        },
      });
  }

  public removeProjectColor(dashboardProjectEntry: UserProject, index: number) {
    this.colorInputToggleArray[index] = false;

    if (dashboardProjectEntry.color == null) {
      this.notificationService.error(`There is no color to delete for project: "${dashboardProjectEntry.name}"`);
      return;
    }

    this.userProjectService
      .deleteProjectColor(dashboardProjectEntry.pid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: _ => {
          // update local cache of project entries
          let updatedDashboardProjectEntry = { ...dashboardProjectEntry };
          updatedDashboardProjectEntry.color = null;
          const newProjectEntries = this.userProjectEntries.slice();
          newProjectEntries[index] = updatedDashboardProjectEntry;
          this.userProjectEntries = newProjectEntries;

          // remove records of this project from color data structures
          if (this.colorBrightnessMap.has(dashboardProjectEntry.pid)) {
            this.colorBrightnessMap.delete(dashboardProjectEntry.pid);
          }
          this.userProjectInputColors[index] = "#ffffff"; // reset color wheel
        },
        error: (err: unknown) => {
          // @ts-ignore
          this.notificationService.error(err.error.message);
        },
      });
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
}
