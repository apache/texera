import { Component, OnInit } from "@angular/core";
import { firstValueFrom } from "rxjs";
import { HubService } from "../../service/hub.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Router } from "@angular/router";
import { SearchService } from "../../../dashboard/service/user/search.service";
import { DashboardEntry, UserInfo } from "../../../dashboard/type/dashboard-entry";
import {
  DASHBOARD_HOME,
  DASHBOARD_HUB_DATASET_RESULT,
  DASHBOARD_HUB_WORKFLOW_RESULT,
} from "../../../app-routing.constant";
import { UserService } from "../../../common/service/user/user.service";

@UntilDestroy()
@Component({
  selector: "texera-landing-page",
  templateUrl: "./landing-page.component.html",
  styleUrls: ["./landing-page.component.scss"],
})
export class LandingPageComponent implements OnInit {
  public isLogin = this.userService.isLogin();
  public currentUid = this.userService.getCurrentUser()?.uid;
  public workflowCount: number = 0;
  public datasetCount: number = 0;
  public topLovedWorkflows: DashboardEntry[] = [];
  public topClonedWorkflows: DashboardEntry[] = [];
  public topLovedDatasets: DashboardEntry[] = [];

  constructor(
    private hubService: HubService,
    private router: Router,
    private searchService: SearchService,
    private userService: UserService
  ) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.isLogin = this.userService.isLogin();
        this.currentUid = this.userService.getCurrentUser()?.uid;
      });
  }

  ngOnInit(): void {
    this.getWorkflowCount();
    this.loadTops();
  }

  async loadTops() {
    try {
      this.topLovedWorkflows = await this.getTopLovedEntries("workflow", "like");
      this.topClonedWorkflows = await this.getTopLovedEntries("workflow", "clone");
      this.topLovedDatasets = await this.getTopLovedEntries("dataset", "like");
    } catch (error) {
      console.error("Failed to load top loved workflows:", error);
    }
  }

  getWorkflowCount(): void {
    this.hubService
      .getCount("workflow")
      .pipe(untilDestroyed(this))
      .subscribe((count: number) => {
        this.workflowCount = count;
      });
    this.hubService
      .getCount("dataset")
      .pipe(untilDestroyed(this))
      .subscribe((count: number) => {
        this.datasetCount = count;
      });
  }

  // todo: same as the function in search. refactor together
  public async getTopLovedEntries(entityType: string, actionType: string): Promise<DashboardEntry[]> {
    const searchResultItems = await firstValueFrom(this.hubService.getTops(entityType, actionType, this.currentUid));

    const userIds = new Set<number>();
    searchResultItems.forEach(i => {
      if (i.workflow) {
        userIds.add(i.workflow.ownerId);
      } else if (i.project) {
        userIds.add(i.project.ownerId);
      } else if (i.dataset) {
        const ownerUid = i.dataset.dataset?.ownerUid;
        if (ownerUid !== undefined) {
          userIds.add(ownerUid);
        }
      }
    });

    let userIdToInfoMap: { [key: number]: UserInfo } = {};
    if (userIds.size > 0) {
      userIdToInfoMap = await firstValueFrom(this.searchService.getUserInfo(Array.from(userIds)));
    }

    return searchResultItems.map(i => {
      let entry: DashboardEntry;

      if (i.workflow) {
        entry = new DashboardEntry(i.workflow);
        const userInfo = userIdToInfoMap[i.workflow.ownerId];
        if (userInfo) {
          entry.setOwnerName(userInfo.userName);
          entry.setOwnerGoogleAvatar(userInfo.googleAvatar ?? "");
        }
      } else if (i.dataset) {
        entry = new DashboardEntry(i.dataset);
        const ownerUid = i.dataset.dataset?.ownerUid;
        if (ownerUid !== undefined) {
          const userInfo = userIdToInfoMap[ownerUid];
          if (userInfo) {
            entry.setOwnerName(userInfo.userName);
            entry.setOwnerGoogleAvatar(userInfo.googleAvatar ?? "");
          }
        }
      } else {
        throw new Error("Unexpected type in SearchResultItem.");
      }

      return entry;
    });
  }

  navigateToSearch(type: string): void {
    let path: string;

    switch (type) {
      case "workflow":
        path = DASHBOARD_HUB_WORKFLOW_RESULT;
        break;
      case "dataset":
        path = DASHBOARD_HUB_DATASET_RESULT;
        break;
      default:
        path = DASHBOARD_HOME;
    }

    this.router.navigate([path]);
  }
}
