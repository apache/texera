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

/*
*  This file defines the url path
*  The workflow workspace is set as default pathu
*  The user dashboard is under path '/dashboard'
*  The saved project is under path '/dashboard/savedproject'
*  The user dictionary is under path '/dashboard/userdictionary'
*/
const routes: Routes = [
  {
    path : '',
    component : WorkspaceComponent
  },
  {
    path : 'dashboard',
    component : DashboardComponent,
    children : [
      {
        path : 'savedproject',
        component : SavedProjectSectionComponent,
      },
      {
        path : 'userdictionary',
        component : UserDictionarySectionComponent
      }
    ]
  },
  {
    path: '**',
    redirectTo: ''
  }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
