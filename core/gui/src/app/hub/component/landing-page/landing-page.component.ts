import { Component, OnInit } from "@angular/core";
import { Observable } from "rxjs";
import { HubWorkflowService } from "../../service/workflow/hub-workflow.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Router } from "@angular/router";
import { DashboardWorkflow } from "../../../dashboard/type/dashboard-workflow.interface";
import { SearchService } from "../../../dashboard/service/user/search.service";
import { DashboardEntry, UserInfo } from "../../../dashboard/type/dashboard-entry";
import { map, switchMap } from "rxjs/operators";
import { DASHBOARD_HUB_WORKFLOW_RESULT } from "../../../app-routing.constant";

@UntilDestroy()
@Component({
  selector: "texera-landing-page",
  templateUrl: "./landing-page.component.html",
  styleUrls: ["./landing-page.component.scss"],
})
export class LandingPageComponent implements OnInit {
  public workflowCount: number = 0;
  public topLovedWorkflows: DashboardEntry[] = [];
  public topClonedWorkflows: DashboardEntry[] = [];

  constructor(
    private hubWorkflowService: HubWorkflowService,
    private router: Router,
    private searchService: SearchService
  ) {}

  ngOnInit(): void {
    this.getWorkflowCount();
    this.fetchTopWorkflows(
      this.hubWorkflowService.getTopLovedWorkflows(),
      workflows => (this.topLovedWorkflows = workflows),
      "Top Loved Workflows"
    );
    this.fetchTopWorkflows(
      this.hubWorkflowService.getTopClonedWorkflows(),
      workflows => (this.topClonedWorkflows = workflows),
      "Top Cloned Workflows"
    );
  }

  getWorkflowCount(): void {
    this.hubWorkflowService
      .getWorkflowCount()
      .pipe(untilDestroyed(this))
      .subscribe((count: number) => {
        this.workflowCount = count;
      });
  }

  /**
   * Helper function to fetch top workflows and associate user info with them.
   * @param workflowsObservable Observable that returns workflows (Top Loved or Top Cloned)
   * @param updateWorkflowsFn Function to update the component's workflow state
   * @param workflowType Label for logging
   */
  fetchTopWorkflows(
    workflowsObservable: Observable<DashboardWorkflow[]>,
    updateWorkflowsFn: (entries: DashboardEntry[]) => void,
    workflowType: string
  ): void {
    workflowsObservable
      .pipe(
        // eslint-disable-next-line rxjs/no-unsafe-takeuntil
        untilDestroyed(this),
        map((workflows: DashboardWorkflow[]) => {
          const userIds = new Set<number>();
          workflows.forEach(workflow => {
            userIds.add(workflow.ownerId);
          });
          return { workflows, userIds: Array.from(userIds) };
        }),
        switchMap(({ workflows, userIds }) =>
          this.searchService.getUserInfo(userIds).pipe(
            map((userIdToInfoMap: { [key: number]: UserInfo }) => {
              const dashboardEntries = workflows.map(workflow => {
                const userInfo = userIdToInfoMap[workflow.ownerId];
                const entry = new DashboardEntry(workflow);
                if (userInfo) {
                  entry.setOwnerName(userInfo.userName);
                  entry.setOwnerGoogleAvatar(userInfo.googleAvatar ?? "");
                }
                return entry;
              });
              return dashboardEntries;
            })
          )
        )
      )
      .subscribe((dashboardEntries: DashboardEntry[]) => {
        updateWorkflowsFn(dashboardEntries);
      });
  }

  navigateToSearch(): void {
    this.router.navigate([DASHBOARD_HUB_WORKFLOW_RESULT]);
  }
}
