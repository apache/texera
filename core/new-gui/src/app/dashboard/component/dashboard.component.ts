import { Component, OnInit } from '@angular/core';

import { UserDictionaryService } from '../service/user-dictionary/user-dictionary.service';
import { StubUserDictionaryService } from '../service/user-dictionary/stub-user-dictionary.service';
import { SavedProjectService } from '../service/saved-project/saved-project.service';
import { StubSavedProjectService } from '../service/saved-project/stub-saved-project.service';

/**
 * dashboardComponent is the component which contains all the subcomponents
 * on the user dashboard. The subcomponents include Top bar, feature bar,
 * and feature container.
 *
 * @author Zhaomin Li
 */
@Component({
  selector: 'texera-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss'],
  providers: [
    { provide: UserDictionaryService, useClass: UserDictionaryService },
    { provide: SavedProjectService, useClass: StubSavedProjectService }
  ]
})
export class DashboardComponent implements OnInit {

  constructor() { }

  ngOnInit() {
  }

}
