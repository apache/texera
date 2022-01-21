import { Component, Input, OnInit } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { FormBuilder, Validators } from "@angular/forms";
import { UserFileService } from "../../../../service/user-file/user-file.service";
import { DashboardUserFileEntry } from "../../../../type/dashboard-user-file-entry";
import { AccessEntry } from "../../../../type/access.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-ngbd-modal-file-share-access",
  templateUrl: "./ngbd-modal-user-file-share-access.component.html",
  styleUrls: ["./ngbd-modal-user-file-share-access.component.scss"],
})
export class NgbdModalUserFileShareAccessComponent implements OnInit {
  @Input() dashboardUserFileEntry!: DashboardUserFileEntry;

  public shareForm = this.formBuilder.group({
    username: ["", [Validators.required]],
    accessLevel: ["", [Validators.required]],
  });

  public allUserFileAccess: ReadonlyArray<AccessEntry> = [];

  public accessLevels = ["read", "write"];

  public fileOwner: string | undefined;

  constructor(
    public activeModal: NgbActiveModal,
    private userFileService: UserFileService,
    private formBuilder: FormBuilder
  ) {}

  ngOnInit(): void {
    this.refreshGrantedUserFileAccessList(this.dashboardUserFileEntry);
  }

  /**
   * get all shared access of the current dashboardUserFileEntry
   * @param dashboardUserFileEntry target/current dashboardUserFileEntry
   */
  public refreshGrantedUserFileAccessList(dashboardUserFileEntry: DashboardUserFileEntry): void {
    this.userFileService
      .getUserFileAccessList(dashboardUserFileEntry)
      .pipe(untilDestroyed(this))
      .subscribe(
        (userFileAccess: ReadonlyArray<AccessEntry>) => {
          const newAccessList: AccessEntry[] = [];
          userFileAccess.map(accessEntry => {
            if (accessEntry.accessLevel === "Owner") {
              this.fileOwner = accessEntry.userName;
            } else {
              newAccessList.push(accessEntry);
            }
          });
          this.allUserFileAccess = newAccessList;
        },
        // @ts-ignore // TODO: fix this with notification component
        (err: unknown) => console.log(err.error)
      );
  }

  /**
   * triggered by clicking the SUBMIT button, offers access based on the input information
   * @param dashboardUserFileEntry target/current file
   */
  public onClickShareUserFile(dashboardUserFileEntry: DashboardUserFileEntry): void {
    if (this.shareForm.get("username")?.invalid) {
      alert("Please Fill in Username");
      return;
    }
    if (this.shareForm.get("accessLevel")?.invalid) {
      alert("Please Select Access Level");
      return;
    }
    const userToShareWith = this.shareForm.get("username")?.value;
    const accessLevel = this.shareForm.get("accessLevel")?.value;
    this.userFileService
      .grantUserFileAccess(dashboardUserFileEntry, userToShareWith, accessLevel)
      .pipe(untilDestroyed(this))
      .subscribe(
        () => this.refreshGrantedUserFileAccessList(dashboardUserFileEntry),
        // @ts-ignore // TODO: fix this with notification component
        (err: unknown) => alert(err.error)
      );
  }

  /**
   * Remove the given user's access to the target file.
   * @param dashboardUserFileEntry target/current file.
   * @param userNameToRemove
   */
  public onClickRemoveUserFileAccess(dashboardUserFileEntry: DashboardUserFileEntry, userNameToRemove: string): void {
    this.userFileService
      .revokeUserFileAccess(dashboardUserFileEntry, userNameToRemove)
      .pipe(untilDestroyed(this))
      .subscribe(
        () => this.refreshGrantedUserFileAccessList(dashboardUserFileEntry),
        // @ts-ignore // TODO: fix this with notification component
        (err: unknown) => alert(err.error)
      );
  }

  /**
   * change form information based on user behavior on UI
   * @param e selected value
   */
  changeType(e: any) {
    this.shareForm.setValue({ accessLevel: e.target.value });
  }
}
