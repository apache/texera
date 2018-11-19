import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { HttpClientModule } from '@angular/common/http';

import { AppRoutingModule } from './app-routing.module';

import { CustomNgMaterialModule } from './common/custom-ng-material.module';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';

import { MaterialDesignFrameworkModule } from 'angular6-json-schema-form';


import { AppComponent } from './app.component';
import { WorkspaceComponent } from './workspace/component/workspace.component';
import { NavigationComponent } from './workspace/component/navigation/navigation.component';
import { OperatorPanelComponent } from './workspace/component/operator-panel/operator-panel.component';
import { PropertyEditorComponent } from './workspace/component/property-editor/property-editor.component';
import { WorkflowEditorComponent } from './workspace/component/workflow-editor/workflow-editor.component';
import { ResultPanelComponent, NgbModalComponent } from './workspace/component/result-panel/result-panel.component';
import { OperatorLabelComponent } from './workspace/component/operator-panel/operator-label/operator-label.component';
import { ResultbarComponent} from './workspace/component/resultbar/resultbar.component'
@NgModule({
  declarations: [
    AppComponent,
    WorkspaceComponent,
    NavigationComponent,
    OperatorPanelComponent,
    PropertyEditorComponent,
    WorkflowEditorComponent,
    ResultPanelComponent,
    NgbModalComponent,
    OperatorLabelComponent,
    ResultbarComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    HttpClientModule,

    CustomNgMaterialModule,
    BrowserAnimationsModule,
    NgbModule.forRoot(),

    MaterialDesignFrameworkModule

  ],
  providers: [ HttpClientModule ],
  bootstrap: [AppComponent],
  // dynamically created component must be placed in the entryComponents attribute
  entryComponents: [ NgbModalComponent ]
})
export class AppModule { }
