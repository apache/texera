import { NgModule } from "@angular/core";
import { RouterModule, Routes } from "@angular/router";
import { environment } from "../environments/environment";
import { DashboardComponent } from "./dashboard/component/dashboard.component";
import { UserWorkflowComponent } from "./dashboard/component/user/user-workflow/user-workflow.component";
import { UserQuotaComponent } from "./dashboard/component/user/user-quota/user-quota.component";
import { UserProjectSectionComponent } from "./dashboard/component/user/user-project/user-project-section/user-project-section.component";
import { UserProjectComponent } from "./dashboard/component/user/user-project/user-project.component";
import { WorkspaceComponent } from "./workspace/component/workspace.component";
import { HomeComponent } from "./hub/component/home/home.component";
import { AuthGuardService } from "./common/service/user/auth-guard.service";
import { AdminUserComponent } from "./dashboard/component/admin/user/admin-user.component";
import { AdminExecutionComponent } from "./dashboard/component/admin/execution/admin-execution.component";
import { AdminGuardService } from "./dashboard/service/admin/guard/admin-guard.service";
import { SearchComponent } from "./dashboard/component/user/search/search.component";
import { FlarumComponent } from "./dashboard/component/user/flarum/flarum.component";
import { AdminGmailComponent } from "./dashboard/component/admin/gmail/admin-gmail.component";
import { UserDatasetExplorerComponent } from "./dashboard/component/user/user-dataset/user-dataset-explorer/user-dataset-explorer.component";
import { UserDatasetComponent } from "./dashboard/component/user/user-dataset/user-dataset.component";
import { HubWorkflowSearchComponent } from "./hub/component/workflow/search/hub-workflow-search.component";
import { HubWorkflowResultComponent } from "./hub/component/workflow/result/hub-workflow-result.component";
import { HubWorkflowComponent } from "./hub/component/workflow/hub-workflow.component";
import { HubWorkflowDetailComponent } from "./hub/component/workflow/detail/hub-workflow-detail.component";

const routes: Routes = [];

if (environment.userSystemEnabled) {
  routes.push({
    path: "dashboard",
    component: DashboardComponent,
    children: [
      {
        path: "home",
        component: HomeComponent,
      },
      {
        path: "hub",
        children: [
          {
            path: "workflow",
            component: HubWorkflowComponent,
            children: [
              {
                path: "search",
                component: HubWorkflowSearchComponent,
              },
              {
                path: "search/result",
                component: HubWorkflowResultComponent,
              },
              {
                path: "search/result/detail",
                component: HubWorkflowDetailComponent,
              },
            ],
          },
        ],
      },
      {
        path: "user",
        canActivate: [AuthGuardService],
        children: [
          {
            path: "project",
            component: UserProjectComponent,
          },
          {
            path: "project/:pid",
            component: UserProjectSectionComponent,
          },
          {
            path: "workspace/:id",
            component: WorkspaceComponent,
          },
          {
            path: "workflow",
            component: UserWorkflowComponent,
          },
          {
            path: "dataset",
            component: UserDatasetComponent,
          },
          {
            path: "dataset/:did",
            component: UserDatasetExplorerComponent,
          },
          {
            path: "dataset/create",
            component: UserDatasetExplorerComponent,
          },
          {
            path: "quota",
            component: UserQuotaComponent,
          },
          {
            path: "search",
            component: SearchComponent,
          },
          {
            path: "discussion",
            component: FlarumComponent,
          },
        ],
      },
      {
        path: "admin",
        canActivate: [AdminGuardService],
        children: [
          {
            path: "user",
            component: AdminUserComponent,
          },
          {
            path: "gmail",
            component: AdminGmailComponent,
          },
          {
            path: "execution",
            component: AdminExecutionComponent,
          },
        ],
      },
    ],
  });

  routes.push({
    path: "",
    redirectTo: "dashboard/user/workflow",
    pathMatch: "full",
  });
} else {
  routes.push({
    path: "",
    component: WorkspaceComponent,
  });
}

// redirect all other paths to index.
routes.push({
  path: "**",
  redirectTo: "dashboard/user/workflow",
});

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule],
})
export class AppRoutingModule {}
