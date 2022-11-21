import { Component, OnInit } from "@angular/core";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { DashboardAdminUserEntry, SortMethod } from "../../../type/dashboard-admin-user-entry";
import { UserService } from "../../../../common/service/user/user.service";
import { AdminUserService } from "../../../service/admin-user/admin-user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import Fuse from "fuse.js";
import {DashboardUserFileEntry} from "../../../type/dashboard-user-file-entry";

@UntilDestroy()
@Component({
  selector: "texera-user-section",
  templateUrl: "./admin-user-section.component.html",
  styleUrls: ["./admin-user-section.component.scss"],
})
export class AdminUserSectionComponent implements OnInit {
  constructor(
    private modalService: NgbModal,
    private adminUserService: AdminUserService,
    private userService: UserService,

  ) {}

  ngOnInit() {
    this.registerDashboardFileEntriesRefresh();
  }
  // variables for file editing / search / sort
  public dashboardUserEntries: ReadonlyArray<DashboardAdminUserEntry> = [];
  public isEditingName: number[] = [];
  public userFileSearchValue: string = "";
  public filteredFilenames: Array<string> = new Array();
  public isTyping: boolean = false;
  public fuse = new Fuse([] as ReadonlyArray<DashboardAdminUserEntry>, {
    shouldSort: true,
    threshold: 0.2,
    location: 0,
    distance: 100,
    minMatchCharLength: 1,
    keys: ["file.name"],
  });
  public sortMethod: SortMethod = SortMethod.UploadTimeDesc;

  public openFileAddComponent() {

  }


  public getUserArray(): ReadonlyArray<DashboardAdminUserEntry> {
    this.sortFileEntries(); // default sorting
    const userArray = this.dashboardUserEntries;
    if (!userArray) {
      return [];
    } else if (this.userFileSearchValue !== "" && !this.isTyping) {
      this.fuse.setCollection(userArray);
      return this.fuse.search(this.userFileSearchValue).map(item => {
        return item.item;
      });
    }
    console.log(userArray);
    return userArray;

  }

  public disableAddButton(): boolean {
    return !this.userService.isLogin();
  }


  public confirmUpdateFileCustomName(
    dashboardUserFileEntry: DashboardUserFileEntry,
    name: string,
    index: number
  ): void {
    const {
      file: { fid },
    } = dashboardUserFileEntry;
    this.adminUserService
      .updateFileName(fid, name)
      .pipe(untilDestroyed(this))
      .subscribe(
        () => this.refreshDashboardFileEntries(),
        (err: unknown) => {
          // @ts-ignore // TODO: fix this with notification component
          this.notificationService.error(err.error.message);
          this.refreshDashboardFileEntries();
        }
      )
      .add(() => (this.isEditingName = this.isEditingName.filter(fileIsEditing => fileIsEditing != index)));
  }


  private registerDashboardFileEntriesRefresh(): void {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.userService.isLogin()) {
          this.refreshDashboardFileEntries();
        } else {
          this.clearDashboardFileEntries();
        }
      });
  }

  private refreshDashboardFileEntries(): void {
    this.adminUserService
      .retrieveDashboardUserFileEntryList()
      .pipe(untilDestroyed(this))
      .subscribe(dashboardUserEntries => {
        this.dashboardUserEntries = dashboardUserEntries;
        this.adminUserService.updateUserFilesChangedEvent();
      });
  }

  private clearDashboardFileEntries(): void {
    this.dashboardUserEntries = [];
    this.adminUserService.updateUserFilesChangedEvent();
  }

  /**
   * Sort the files according to sortMethod variable
   */
  public sortFileEntries(): void {
    switch (this.sortMethod) {
      case SortMethod.NameAsc:
        this.ascSort();
        break;
      case SortMethod.NameDesc:
        this.dscSort();
        break;
      case SortMethod.UploadTimeAsc:
        this.timeSortAsc();
        break;
      case SortMethod.UploadTimeDesc:
        this.timeSortDesc();
        break;
    }
  }

  /**
   * sort the workflow by owner name + file name in ascending order
   */
  public ascSort(): void {
    this.sortMethod = SortMethod.NameAsc;
    this.dashboardUserEntries = this.dashboardUserEntries
      .slice()
      .sort((t1, t2) =>
        t1.name.toLowerCase().localeCompare(t2.name.toLowerCase())
      );
  }

  /**
   * sort the project by owner name + file name in descending order
   */
  public dscSort(): void {
    this.sortMethod = SortMethod.NameDesc;
    this.dashboardUserEntries = this.dashboardUserEntries
      .slice()
      .sort((t1, t2) =>
        t2.name.toLowerCase().localeCompare(t1.name.toLowerCase())
      );
  }

  /**
   * sort the project by upload time in descending order
   */
  public timeSortDesc(): void {
    this.sortMethod = SortMethod.UploadTimeDesc;
    this.dashboardUserEntries = this.dashboardUserEntries
      .slice()
      .sort((left, right) =>
         right.uid - left.uid
      );
  }

  /**
   * sort the project by upload time in ascending order
   */
  public timeSortAsc(): void {
    this.sortMethod = SortMethod.UploadTimeAsc;
    this.dashboardUserEntries = this.dashboardUserEntries
      .slice()
      .sort((left, right) =>
        left.uid - right.uid
      );
  }
}
