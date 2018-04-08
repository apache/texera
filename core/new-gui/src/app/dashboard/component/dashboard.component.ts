import { Component, OnInit } from '@angular/core';

import { UserDictionaryService } from '../service/user-dictionary/user-dictionary.service';
import { StubUserDictionaryService } from '../service/user-dictionary/stub-user-dictionary.service';
import { SavedProjectService } from '../service/saved-project/saved-project.service';
import { StubSavedProjectService } from '../service/saved-project/stub-saved-project.service';


@Component({
  selector: 'texera-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss'],
  providers: [
    UserDictionaryService,
    StubUserDictionaryService,
    SavedProjectService,
    StubSavedProjectService
  ]
})
export class DashboardComponent implements OnInit {

  constructor() { }

  ngOnInit() {
  }

}
