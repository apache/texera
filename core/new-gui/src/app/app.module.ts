import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { HttpClientModule } from '@angular/common/http';
import { AppRoutingModule } from './app-routing.module';
import { environment } from './../environments/environment';

import { CustomNgMaterialModule } from './common/custom-ng-material.module';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgbModule, NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';
import { RouterModule } from '@angular/router';
import { TourNgBootstrapModule } from 'ngx-tour-ng-bootstrap';
import { LoggerModule, NgxLoggerLevel } from 'ngx-logger';

import { MaterialDesignFrameworkModule } from 'angular6-json-schema-form';
import { NgxJsonViewerModule } from 'ngx-json-viewer';

import { MatTooltipModule } from '@angular/material';

import { AppComponent } from './app.component';
import { WorkspaceComponent } from './workspace/component/workspace.component';
import { NavigationComponent} from './workspace/component/navigation/navigation.component';

import { OperatorPanelComponent } from './workspace/component/operator-panel/operator-panel.component';
import { PropertyEditorComponent } from './workspace/component/property-editor/property-editor.component';
import { WorkflowEditorComponent } from './workspace/component/workflow-editor/workflow-editor.component';
import { ResultPanelComponent, NgbModalComponent } from './workspace/component/result-panel/result-panel.component';
import { OperatorLabelComponent } from './workspace/component/operator-panel/operator-label/operator-label.component';
import { ProductTourComponent } from './workspace/component/product-tour/product-tour.component';
import { MiniMapComponent } from './workspace/component/mini-map/mini-map.component';

import { ResultPanelToggleComponent } from './workspace/component/result-panel-toggle/result-panel-toggle.component';

import { DashboardComponent } from './dashboard/component/dashboard.component';
import { TopBarComponent } from './dashboard/component/top-bar/top-bar.component';
import { UserAccountIconComponent } from './dashboard/component/top-bar/user-account-icon/user-account-icon.component';
import { FeatureBarComponent } from './dashboard/component/feature-bar/feature-bar.component';
import { FeatureContainerComponent } from './dashboard/component/feature-container/feature-container.component';
import {
  SavedProjectSectionComponent
} from './dashboard/component/feature-container/saved-project-section/saved-project-section.component';
import {
  NgbdModalAddProjectComponent
} from './dashboard/component/feature-container/saved-project-section/ngbd-modal-add-project/ngbd-modal-add-project.component';
import {
  NgbdModalDeleteProjectComponent
} from './dashboard/component/feature-container/saved-project-section/ngbd-modal-delete-project/ngbd-modal-delete-project.component';

import {
  RunningJobSectionComponent
} from './dashboard/component/feature-container/running-job-section/running-job-section.component';
import {
  UserDictionarySectionComponent
} from './dashboard/component/feature-container/user-dictionary-section/user-dictionary-section.component';
import {
  NgbdModalResourceViewComponent
} from './dashboard/component/feature-container/user-dictionary-section/ngbd-modal-resource-view/ngbd-modal-resource-view.component';
import {
  NgbdModalResourceAddComponent
} from './dashboard/component/feature-container/user-dictionary-section/ngbd-modal-resource-add/ngbd-modal-resource-add.component';
import {
  NgbdModalResourceDeleteComponent
} from './dashboard/component/feature-container/user-dictionary-section/ngbd-modal-resource-delete/ngbd-modal-resource-delete.component';

import { ResourceSectionComponent } from './dashboard/component/feature-container/resource-section/resource-section.component';

import { FileUploadModule } from 'ng2-file-upload';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { FormlyBootstrapModule } from '@ngx-formly/bootstrap';
import { FormlyModule, FormlyFieldConfig } from '@ngx-formly/core';

import { ArrayTypeComponent } from './common/array.type';
import { ObjectTypeComponent } from './common/object.type';
import { MultiSchemaTypeComponent } from './common/multischema.type';
import { NullTypeComponent } from './common/null.type';



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

    DashboardComponent,
    TopBarComponent,
    UserAccountIconComponent,
    FeatureBarComponent,
    FeatureContainerComponent,

    SavedProjectSectionComponent,
    NgbdModalAddProjectComponent,
    NgbdModalDeleteProjectComponent,

    RunningJobSectionComponent,
    UserDictionarySectionComponent,
    NgbdModalResourceViewComponent,
    NgbdModalResourceAddComponent,
    NgbdModalResourceDeleteComponent,

    ResourceSectionComponent,

    NgbModalComponent,
    OperatorLabelComponent,
    ProductTourComponent,
    MiniMapComponent,
    ResultPanelToggleComponent,

    ArrayTypeComponent,
    ObjectTypeComponent,
    MultiSchemaTypeComponent,
    NullTypeComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    HttpClientModule,

    MatTooltipModule,
    NgxJsonViewerModule,
    CustomNgMaterialModule,
    BrowserAnimationsModule,
    NgbModule,
    NgbPopoverModule,
    RouterModule.forRoot([]),
    TourNgBootstrapModule.forRoot(),

    MaterialDesignFrameworkModule,
    FileUploadModule,
    FormsModule,
    ReactiveFormsModule,
    LoggerModule.forRoot({level: environment.production ? NgxLoggerLevel.ERROR : NgxLoggerLevel.DEBUG, serverLogLevel: NgxLoggerLevel.OFF}),
    FormlyModule.forRoot({
      validationMessages: [
        { name: 'required', message: 'This field is required' },
        { name: 'null', message: 'should be null' },
        { name: 'minlength', message: 'minlengthValidationMessage' },
        { name: 'maxlength', message: 'maxlengthValidationMessage' },
        { name: 'min', message: 'minValidationMessage' },
        { name: 'max', message: 'maxValidationMessage' },
        { name: 'multipleOf', message: 'multipleOfValidationMessage' },
        { name: 'exclusiveMinimum', message: 'exclusiveMinimumValidationMessage' },
        { name: 'exclusiveMaximum', message: 'exclusiveMaximumValidationMessage' },
        { name: 'minItems', message: 'minItemsValidationMessage' },
        { name: 'maxItems', message: 'maxItemsValidationMessage' },
        { name: 'uniqueItems', message: 'should NOT have duplicate items' },
        { name: 'const', message: 'constValidationMessage' },
      ],
      types: [
        { name: 'string', extends: 'input' },
        {
          name: 'number',
          extends: 'input',
          defaultOptions: {
            templateOptions: {
              type: 'number',
            },
          },
        },
        {
          name: 'integer',
          extends: 'input',
          defaultOptions: {
            templateOptions: {
              type: 'number',
            },
          },
        },
        { name: 'boolean', extends: 'checkbox' },
        { name: 'enum', extends: 'select' },
        { name: 'null', component: NullTypeComponent, wrappers: ['form-field'] },
        { name: 'array', component: ArrayTypeComponent },
        { name: 'object', component: ObjectTypeComponent },
        { name: 'multischema', component: MultiSchemaTypeComponent },
      ],
    }),
    FormlyBootstrapModule,


  ],
  entryComponents: [
    NgbdModalAddProjectComponent,
    NgbdModalDeleteProjectComponent,
    NgbdModalResourceViewComponent,
    NgbdModalResourceAddComponent,
    NgbdModalResourceDeleteComponent,
    NgbModalComponent
  ],
  providers: [HttpClientModule],
  bootstrap: [AppComponent],
  // dynamically created component must be placed in the entryComponents attribute
})
export class AppModule { }
