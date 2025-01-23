import { Component, OnInit } from "@angular/core";
import { UserProjectService } from "../../../../service/user/project/user-project.service";
import { ActivatedRoute } from "@angular/router";
import { DashboardFile } from "../../../../type/dashboard-file.interface";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { isDefined } from "../../../../../common/util/predicate";

@UntilDestroy()
@Component({
  selector: "texera-user-project-section",
  templateUrl: "./user-project-section.component.html",
  styleUrls: ["./user-project-section.component.scss"],
})
export class UserProjectSectionComponent implements OnInit {
  // information from the database about this project
  public pid?: number = undefined;
  public name: string = "";
  public description: string = "";
  public ownerID: number = 0;
  public creationTime: number = 0;
  public accessLevel: string = "READ";
  public color: string | null = null;

  // information for modifying project color
  public inputColor: string = "#ffffff"; // needs to have a '#' in front, as it is used by ngx-color-picker
  public colorIsBright: boolean = false;
  public projectDataIsLoaded: boolean = false;
  public colorPickerIsSelected: boolean = false;
  public updateProjectStatus = ""; // track any updates to user project for child components to rerender

  constructor(
    private userProjectService: UserProjectService,
    private activatedRoute: ActivatedRoute,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    // extract passed PID from parameter and re-render page if necessary
    this.activatedRoute.url.pipe(untilDestroyed(this)).subscribe(url => {
      if (url.length == 2 && url[1].path) {
        this.pid = parseInt(url[1].path);

        this.getUserProjectMetadata();
        this.userProjectService.refreshFilesOfProject(this.pid); // TODO : remove after refactoring file section
      }
    });

    // otherwise no project ID, no project to load
  }

  public getUserProjectFilesArray(): ReadonlyArray<DashboardFile> {
    const fileArray = this.userProjectService.getProjectFiles();
    if (!fileArray) {
      return [];
    }
    return fileArray;
  }

  public updateProjectColor(color: string) {
    color = color.substring(1);
    this.colorPickerIsSelected = false;

    if (UserProjectService.isInvalidColorFormat(color)) {
      this.notificationService.error("Cannot update project color. Color must be in valid HEX format");
      return;
    }

    if (this.color === color) {
      return;
    }
    if (!isDefined(this.pid)) {
      return;
    }
    this.userProjectService
      .updateProjectColor(this.pid, color)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.color = color;
          this.colorIsBright = UserProjectService.isLightColor(this.color);
          this.updateProjectStatus = "updated project color"; // cause workflow / file components to update project filtering list
        },
        error: (e: unknown) => this.notificationService.error((e as Error).message),
      });
  }

  public removeProjectColor() {
    this.colorPickerIsSelected = false;

    if (this.color == null) {
      this.notificationService.error("There is no color to delete for this project");
      return;
    }

    this.userProjectService
      .deleteProjectColor(this.pid!)
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.color = null;
        this.inputColor = "#ffffff";
        this.updateProjectStatus = "removed project color"; // cause workflow / file components to update project filtering list
      });
  }

  private getUserProjectMetadata() {
    if (!isDefined(this.pid)) {
      return;
    }
    this.userProjectService
      .retrieveProject(this.pid)
      .pipe(untilDestroyed(this))
      .subscribe(project => {
        this.name = project.name;
        this.ownerID = project.ownerId;
        this.creationTime = project.creationTime;
        if (project.color != null) {
          this.color = project.color;
          this.inputColor = "#" + project.color;
          this.colorIsBright = UserProjectService.isLightColor(project.color);
        }
        this.projectDataIsLoaded = true;
      });

    this.userProjectService
      .getProjectList()
      .pipe(untilDestroyed(this))
      .subscribe(userProjectList => {
        if (userProjectList != null && userProjectList.length > 0) {
          // calculate whether project colors are light or dark
          const projectColorBrightnessMap: Map<number, boolean> = new Map();
          userProjectList.forEach(userProject => {
            if (userProject.color != null) {
              projectColorBrightnessMap.set(userProject.pid, UserProjectService.isLightColor(userProject.color));
            }

            // get single project information
            if (userProject.pid === this.pid) {
              this.name = userProject.name;
              this.description = userProject.description;
              this.ownerID = userProject.ownerId;
              this.creationTime = userProject.creationTime;
              this.accessLevel = userProject.accessLevel;
              if (userProject.color != null) {
                this.color = userProject.color;
                this.inputColor = "#" + userProject.color;
                this.colorIsBright = UserProjectService.isLightColor(userProject.color);
              }
            }
          });
          this.projectDataIsLoaded = true;
        }
      });
  }
}
