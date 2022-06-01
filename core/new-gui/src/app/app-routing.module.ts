import { NgModule } from "@angular/core";
import { RouterModule, Routes } from "@angular/router";
import { environment } from "../environments/environment";

import { DashboardComponent } from "./dashboard/component/dashboard.component";
import { SavedWorkflowSectionComponent } from "./dashboard/component/feature-container/saved-workflow-section/saved-workflow-section.component";
import { UserFileSectionComponent } from "./dashboard/component/feature-container/user-file-section/user-file-section.component";
import { UserProjectSectionComponent } from "./dashboard/component/feature-container/user-project-list/user-project-section/user-project-section.component";
import { UserProjectListComponent } from "./dashboard/component/feature-container/user-project-list/user-project-list.component";

import { WorkspaceComponent } from "./workspace/component/workspace.component";

/*
 *  This file defines the url path
 *  The workflow workspace is set as default path
 */
const routes: Routes = [
  {
    path: "",
    component: WorkspaceComponent,
  },
  {
    path: "workflow/:id",
    component: WorkspaceComponent,
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
    children: [
      {
        path: "user-project",
        component: UserProjectListComponent,
      },
      {
        path: "user-project/:pid",
        component: UserProjectSectionComponent,
      },
      {
        path: "workflow",
        component: SavedWorkflowSectionComponent,
      },
      {
        path: "user-file",
        component: UserFileSectionComponent,
      },
    ],
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
