import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { HttpClientModule } from '@angular/common/http';

import { AppRoutingModule } from './app-routing.module';

import { CustomNgMaterialModule } from './common/custom-ng-material.module';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';

import {
  MaterialDesignFrameworkModule, JsonSchemaFormModule, JsonSchemaFormService,
  FrameworkLibraryService, WidgetLibraryService, Framework, MaterialDesignFramework
} from 'angular2-json-schema-form';

import { AppComponent } from './app.component';
import { WorkspaceComponent } from './workspace/component/workspace.component';
import { NavigationComponent } from './workspace/component/navigation/navigation.component';
import { OperatorPanelComponent } from './workspace/component/operator-panel/operator-panel.component';
import { PropertyEditorComponent } from './workspace/component/property-editor/property-editor.component';
import { WorkflowEditorComponent } from './workspace/component/workflow-editor/workflow-editor.component';
import { ResultPanelComponent } from './workspace/component/result-panel/result-panel.component';
import { OperatorLabelComponent } from './workspace/component/operator-panel/operator-label/operator-label.component';

// remove annoying Angular material hammer js warning
import 'hammerjs';

@NgModule({
  declarations: [
    AppComponent,
    WorkspaceComponent,
    NavigationComponent,
    OperatorPanelComponent,
    PropertyEditorComponent,
    WorkflowEditorComponent,
    ResultPanelComponent,
    OperatorLabelComponent,
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    HttpClientModule,

    CustomNgMaterialModule,
    BrowserAnimationsModule,
    NgbModule.forRoot(),

    MaterialDesignFrameworkModule,
    // workaround to import the angular json schema module to avoid errros in compliation
    // https://github.com/dschnelldavis/angular2-json-schema-form/issues/189#issuecomment-365971521
    {
      ngModule: JsonSchemaFormModule,
      providers: [
          JsonSchemaFormService,
          FrameworkLibraryService,
          WidgetLibraryService,
          {provide: Framework, useClass: MaterialDesignFramework, multi: true}
      ]
    },

  ],
  providers: [ HttpClientModule ],
  bootstrap: [AppComponent]
})
export class AppModule { }
