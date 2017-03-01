import { NgModule }      from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppComponent }  from './app.component';
import { NavigationBarComponent }   from './navigation-bar.component';
import { TheFlowchartComponent }   from './the-flowchart.component';
import { OperatorBarComponent }   from './operator-bar.component';

import { DropdownModule } from 'ng2-bootstrap';
import { Ng2Bs3ModalModule } from 'ng2-bs3-modal/ng2-bs3-modal';

@NgModule({
  imports:      [ DropdownModule.forRoot(),
      Ng2Bs3ModalModule,
      BrowserModule
	],
  declarations: [ AppComponent,
		NavigationBarComponent,
		TheFlowchartComponent,
		OperatorBarComponent
	],
  bootstrap:    [ AppComponent ]
})
export class AppModule { }