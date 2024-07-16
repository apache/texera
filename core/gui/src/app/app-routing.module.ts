import { NgModule } from "@angular/core";
import { RouterModule, Routes } from "@angular/router";
import { environment } from "../environments/environment";
import { DashboardComponent } from "./dashboard/component/dashboard.component";
import { UserWorkflowComponent } from "./dashboard/component/user/user-workflow/user-workflow.component";
import { UserFileComponent } from "./dashboard/component/user/user-file/user-file.component";
import { UserQuotaComponent } from "./dashboard/component/user/user-quota/user-quota.component";
import { UserProjectSectionComponent } from "./dashboard/component/user/user-project/user-project-section/user-project-section.component";
import { UserProjectComponent } from "./dashboard/component/user/user-project/user-project.component";
import { WorkspaceComponent } from "./workspace/component/workspace.component";
import { HomeComponent } from "./home/component/home.component";
import { AuthGuardService } from "./common/service/user/auth-guard.service";
import { AdminUserComponent } from "./dashboard/component/admin/user/admin-user.component";
import { AdminExecutionComponent } from "./dashboard/component/admin/execution/admin-execution.component";
import { AdminGuardService } from "./dashboard/service/admin/guard/admin-guard.service";
import { SearchComponent } from "./dashboard/component/user/search/search.component";
import { FlarumComponent } from "./dashboard/component/user/flarum/flarum.component";
import { AdminGmailComponent } from "./dashboard/component/admin/gmail/admin-gmail.component";
import { UserDatasetExplorerComponent } from "./dashboard/component/user/user-dataset/user-dataset-explorer/user-dataset-explorer.component";
import { UserDatasetComponent } from "./dashboard/component/user/user-dataset/user-dataset.component";
/*
 *  This file defines the url path
 *  The workflow workspace is set as default path
 */
const routes: Routes = [
  {
    path: "",
    component: WorkspaceComponent,
    canActivate: [AuthGuardService],
  },
  {
    path: "workflow/:id",
    component: WorkspaceComponent,
    canActivate: [AuthGuardService],
  },
];
if (environment.userSystemEnabled) {
  /*
   *  The user dashboard is under path '/dashboard'
   *  The saved workflow is under path '/dashboard/workflow'
   *  The user file is under path '/dashboard/user-file'
   *  The user dictionary is under path '/dashboard/user-dictionary'
   *  The user project list is under path '/dashboard/project'
   *  The single user project is under path 'dashboard/project/{pid}'
   */
  routes.push({
    path: "dashboard",
    component: DashboardComponent,
    canActivate: [AuthGuardService],
    children: [
      {
        path: "user-project",
        component: UserProjectComponent,
      },
      {
        path: "user-project/:pid",
        component: UserProjectSectionComponent,
      },
      {
        path: "workflow",
        component: UserWorkflowComponent,
      },
      {
        path: "user-file",
        component: UserFileComponent,
      },
      {
        path: "dataset",
        component: UserDatasetComponent,
      },
      // the below two URLs route to the same Component. The component will render the page accordingly
      {
        path: "dataset/:did",
        component: UserDatasetExplorerComponent,
      },
      {
        path: "dataset/create",
        component: UserDatasetExplorerComponent,
      },
      {
        path: "user-quota",
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
      {
        path: "admin/user",
        component: AdminUserComponent,
        canActivate: [AdminGuardService],
      },
      {
        path: "admin/gmail",
        component: AdminGmailComponent,
        canActivate: [AdminGuardService],
      },
      {
        path: "admin/execution",
        component: AdminExecutionComponent,
        canActivate: [AdminGuardService],
      },
    ],
  });

  routes.push({
    path: "home",
    component: HomeComponent,
  });
}
// redirect all other paths to index.
routes.push({
  path: "**",
  redirectTo: "",
});
@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule],
})
export class AppRoutingModule {}
