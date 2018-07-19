import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { HttpClientModule } from '@angular/common/http';
import { Observable } from 'rxjs/Rx';

import { FlexLayoutModule } from '@angular/flex-layout';
import { CustomNgMaterialModule } from './common/custom-ng-material.module';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { NgxBootstrapProductTourModule } from 'ngx-bootstrap-product-tour';
import { RouterModule } from '@angular/router';

import {
  JsonSchemaFormModule, MaterialDesignFrameworkModule, Bootstrap4FrameworkModule
} from 'angular2-json-schema-form';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { WorkspaceComponent } from './workspace/component/workspace.component';
import { WorkflowEditorComponent } from './workspace/component/workflow-editor/workflow-editor.component';
import { NavigationComponent } from './workspace/component/navigation/navigation.component';
import { PropertyEditorComponent } from './workspace/component/property-editor/property-editor.component';
import { OperatorViewComponent } from './workspace/component/operator-view/operator-view.component';
import { ResultViewComponent } from './workspace/component/result-view/result-view.component';
import { OperatorLabelComponent } from './workspace/component/operator-view/operator-label/operator-label.component';
import { ReactiveFormsModule } from '@angular/forms';
import { FormsModule } from '@angular/forms';
import { startWith } from 'rxjs/operators/startWith';
import { map } from 'rxjs/operators/map';
import { ProductTourComponent } from './workspace/component/product-tour/product-tour.component';

@NgModule({
  declarations: [
    AppComponent,
    WorkspaceComponent,
    WorkflowEditorComponent,
    NavigationComponent,
    PropertyEditorComponent,
    OperatorViewComponent,
    ResultViewComponent,
    OperatorLabelComponent,
    ProductTourComponent,
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    HttpClientModule,

    FlexLayoutModule,
    CustomNgMaterialModule,
    BrowserAnimationsModule,

    MaterialDesignFrameworkModule,
    Bootstrap4FrameworkModule,
    JsonSchemaFormModule.forRoot(MaterialDesignFrameworkModule),
    ReactiveFormsModule,
    FormsModule,
    NgxBootstrapProductTourModule.forRoot(),
    NgbModule.forRoot()
  ],
  providers: [ HttpClientModule ],
  bootstrap: [AppComponent]
})
export class AppModule { }
