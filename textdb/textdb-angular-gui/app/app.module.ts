import { NgModule }      from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule }   from '@angular/forms';

import { AppComponent }  from './app.component';
import { NavigationBarComponent }   from './navigation-bar.component';
import { TheFlowchartComponent }   from './the-flowchart.component';
import { OperatorBarComponent }   from './operator-bar.component';
import { SideBarComponent }   from './side-bar.component';

import { DropdownModule } from 'ng2-bootstrap';

@NgModule({
  imports:      [ DropdownModule.forRoot(),
      BrowserModule,
      FormsModule
	],
  declarations: [ AppComponent,
		NavigationBarComponent,
		TheFlowchartComponent,
		OperatorBarComponent,
        SideBarComponent
	],
  bootstrap:    [ AppComponent ]
})
export class AppModule { }