import { Component, OnInit } from "@angular/core";
import { Observable } from "rxjs";
import { HubService } from "../../service/hub.service";
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
  public deploymentCommit: string = 'Commit unavailable';
  public commitPrefix: string = '';
  public issueNumber: string = '';
  public commitSuffix: string = '';
  public lastDeployTime: string = "";
  public workflowCount: number = 0;
  public topLovedWorkflows: DashboardEntry[] = [];
  public topClonedWorkflows: DashboardEntry[] = [];

  constructor(
    private hubService: HubService,
    private router: Router,
    private searchService: SearchService
  ) {}

  ngOnInit(): void {
    this.getWorkflowCount();
    this.fetchTopWorkflows(
      this.hubService.getTopLovedWorkflows(),
      workflows => (this.topLovedWorkflows = workflows),
      "Top Loved Workflows"
    );
    this.fetchTopWorkflows(
      this.hubService.getTopClonedWorkflows(),
      workflows => (this.topClonedWorkflows = workflows),
      "Top Cloned Workflows"
    );
    this.hubWorkflowService.getGitCommit().subscribe(commit =>{
      // Split the text into parts
      this.deploymentCommit = commit
      const match = this.deploymentCommit.match(/(.*)(#\d+)(.*)/);
      if (match) {
        this.commitPrefix = match[1];
        this.issueNumber = match[2];
        this.commitSuffix = match[3];
      } else {
        this.commitPrefix = this.deploymentCommit;
      }

    })
    this.hubWorkflowService.getLastDeploy().subscribe(deployTime => this.lastDeployTime = deployTime)
  }

  getWorkflowCount(): void {
    this.hubService
      .getCount("workflow")
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
