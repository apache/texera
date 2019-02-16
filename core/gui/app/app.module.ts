import { NgModule }      from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule }   from '@angular/forms';
import { HttpModule }    from '@angular/http';

import { AppComponent }  from './app.component';
import { TheFlowchartComponent }   from './flowchart/the-flowchart.component';
import { NavigationBarComponent }   from './navigation/navigation-bar.component';
import { OperatorBarComponent }   from './operatorbar/operator-bar.component';
import { SideBarComponent }   from './sidebar/side-bar.component';
import { ResultBarComponent } from './resultbar/result-bar.component';

import { BsDropdownModule } from 'ng2-bootstrap/';
import { Ng2Bs3ModalModule } from 'ng2-bs3-modal/ng2-bs3-modal';

@NgModule({
  imports:      [ BsDropdownModule.forRoot(),
      BrowserModule,
      FormsModule,
      HttpModule,
      Ng2Bs3ModalModule,
	],
  declarations: [ AppComponent,
    TheFlowchartComponent,
    NavigationBarComponent,
    OperatorBarComponent,
    SideBarComponent,
    ResultBarComponent,
	],
  bootstrap:    [ AppComponent ]
})
export class AppModule { }
