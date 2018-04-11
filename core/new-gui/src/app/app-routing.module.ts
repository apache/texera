import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { WorkspaceComponent } from './workspace/component/workspace.component';

import { DashboardComponent } from './dashboard/component/dashboard.component';
import {
  SavedProjectSectionComponent
} from './dashboard/component/feature-container/saved-project-section/saved-project-section.component';
import {
  UserDictionarySectionComponent
} from './dashboard/component/feature-container/user-dictionary-section/user-dictionary-section.component';

const routes: Routes = [
  {
    path : '',
    component : WorkspaceComponent
  },
  {
    path : 'Dashboard',
    component : DashboardComponent,
    children : [
      {
        path : 'SavedProject',
        component : SavedProjectSectionComponent,
      },
      {
        path : 'UserDictionary',
        component : UserDictionarySectionComponent
      }
    ]
  }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
