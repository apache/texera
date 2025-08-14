/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { NgModule } from "@angular/core";
import { RouterModule, Routes } from "@angular/router";
import { DashboardComponent } from "./dashboard/component/dashboard.component";
import { UserWorkflowComponent } from "./dashboard/component/user/user-workflow/user-workflow.component";
import { UserQuotaComponent } from "./dashboard/component/user/user-quota/user-quota.component";
import { UserProjectSectionComponent } from "./dashboard/component/user/user-project/user-project-section/user-project-section.component";
import { UserProjectComponent } from "./dashboard/component/user/user-project/user-project.component";
import { WorkspaceComponent } from "./workspace/component/workspace.component";
import { AboutComponent } from "./hub/component/about/about.component";
import { AuthGuardService } from "./common/service/user/auth-guard.service";
import { AdminUserComponent } from "./dashboard/component/admin/user/admin-user.component";
import { AdminExecutionComponent } from "./dashboard/component/admin/execution/admin-execution.component";
import { AdminGuardService } from "./dashboard/service/admin/guard/admin-guard.service";
import { SearchComponent } from "./dashboard/component/user/search/search.component";
import { FlarumComponent } from "./dashboard/component/user/flarum/flarum.component";
import { AdminGmailComponent } from "./dashboard/component/admin/gmail/admin-gmail.component";
import { DatasetDetailComponent } from "./dashboard/component/user/user-dataset/user-dataset-explorer/dataset-detail.component";
import { UserDatasetComponent } from "./dashboard/component/user/user-dataset/user-dataset.component";
import { HubWorkflowDetailComponent } from "./hub/component/workflow/detail/hub-workflow-detail.component";
import { LandingPageComponent } from "./hub/component/landing-page/landing-page.component";
import { DASHBOARD_USER_WORKFLOW } from "./app-routing.constant";
import { HubSearchResultComponent } from "./hub/component/hub-search-result/hub-search-result.component";
import { AdminSettingsComponent } from "./dashboard/component/admin/settings/admin-settings.component";
import { inject } from "@angular/core";
import { GuiConfigService } from "./common/service/gui-config.service";
import { Router, CanActivateFn } from "@angular/router";
import { DASHBOARD_ABOUT } from "./app-routing.constant";

const rootRedirectGuard: CanActivateFn = () => {
  const config = inject(GuiConfigService);
  const router = inject(Router);
  try {
    if (config.env.userSystemEnabled) {
      return router.parseUrl(DASHBOARD_ABOUT);
    }
  } catch {
    // config not loaded yet, swallow the error and let the app handle it
  }
  return true;
};

const routes: Routes = [];

routes.push({
  path: "dashboard",
  component: DashboardComponent,
  children: [
    {
      path: "home",
      component: LandingPageComponent,
    },
    {
      path: "about",
      component: AboutComponent,
    },
    {
      path: "hub",
      children: [
        {
          path: "workflow",
          children: [
            {
              path: "result",
              component: HubSearchResultComponent,
            },
            {
              path: "result/detail/:id",
              component: HubWorkflowDetailComponent,
            },
          ],
        },
        {
          path: "dataset",
          children: [
            {
              path: "result",
              component: HubSearchResultComponent,
            },
            {
              path: "result/detail/:did",
              component: DatasetDetailComponent,
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
          path: "workflow",
          component: UserWorkflowComponent,
        },
        {
          path: "workflow/:id",
          component: WorkspaceComponent,
        },
        {
          path: "dataset",
          component: UserDatasetComponent,
        },
        {
          path: "dataset/:did",
          component: DatasetDetailComponent,
        },
        {
          path: "dataset/create",
          component: DatasetDetailComponent,
        },
        {
          path: "quota",
          component: UserQuotaComponent,
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
        {
          path: "settings",
          component: AdminSettingsComponent,
        },
      ],
    },
    {
      path: "search",
      component: SearchComponent,
    },
  ],
});

// default route renders the workspace editor directly; if userSystem is enabled at runtime,
// AppComponent will navigate to DASHBOARD_ABOUT instead.
routes.push({
  path: "",
  component: WorkspaceComponent,
  canActivate: [rootRedirectGuard],
});

// redirect all other paths to index.
routes.push({
  path: "**",
  redirectTo: DASHBOARD_USER_WORKFLOW,
});

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule],
})
export class AppRoutingModule {}
