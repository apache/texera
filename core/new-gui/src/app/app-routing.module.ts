import { NgModule } from "@angular/core";
import { RouterModule, Routes } from "@angular/router";
import { environment } from "../environments/environment";
import { DashboardComponent } from "./dashboard/user/component/dashboard.component";
import { UserWorkflowComponent } from "./dashboard/user/component/user-workflow/user-workflow.component";
import { UserFileComponent } from "./dashboard/user/component/user-file/user-file.component";
import { UserQuotaComponent } from "./dashboard/user/component/user-quota/user-quota.component";
import { UserProjectSectionComponent } from "./dashboard/user/component/user-project/user-project-section/user-project-section.component";
import { UserProjectComponent } from "./dashboard/user/component/user-project/user-project.component";
import { WorkspaceComponent } from "./workspace/component/workspace.component";
import { HomeComponent } from "./home/component/home.component";
import { AuthGuardService } from "./common/service/user/auth-guard.service";
import { AdminUserComponent } from "./dashboard/admin/component/user/admin-user.component";
import { AdminExecutionComponent } from "./dashboard/admin/component/execution/admin-execution.component";
import { AdminGuardService } from "./dashboard/admin/service/admin-guard.service";
import { SearchComponent } from "./dashboard/user/component/search/search.component";
import { GmailComponent } from "./dashboard/admin/component/gmail/gmail.component";
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
        path: "user-quota",
        component: UserQuotaComponent,
      },
      {
        path: "search",
        component: SearchComponent,
      },
      {
        path: "admin/user",
        component: AdminUserComponent,
        canActivate: [AdminGuardService],
      },
      {
        path: "admin/gmail",
        component: GmailComponent,
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
  imports: [RouterModule.forRoot(routes, { relativeLinkResolution: "legacy" })],
  exports: [RouterModule],
})
export class AppRoutingModule {}
