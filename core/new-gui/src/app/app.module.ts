import { registerLocaleData, DatePipe } from "@angular/common";
import { HTTP_INTERCEPTORS, HttpClientModule } from "@angular/common/http";
import en from "@angular/common/locales/en";
import { NgModule } from "@angular/core";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { MatDialogModule } from "@angular/material/dialog";
import { MatTooltipModule } from "@angular/material/tooltip";
import { BrowserModule } from "@angular/platform-browser";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { RouterModule } from "@angular/router";
import { NgbModule, NgbPopoverModule } from "@ng-bootstrap/ng-bootstrap";
import { FormlyModule } from "@ngx-formly/core";
import { FormlyMaterialModule } from "@ngx-formly/material";
import { FormlyMatDatepickerModule } from "@ngx-formly/material/datepicker";
import { NgPipesModule } from "ngx-pipes";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzCollapseModule } from "ng-zorro-antd/collapse";
import { NzDatePickerModule } from "ng-zorro-antd/date-picker";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzFormModule } from "ng-zorro-antd/form";
import { en_US, NZ_I18N } from "ng-zorro-antd/i18n";
import { NzAutocompleteModule } from "ng-zorro-antd/auto-complete";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzInputModule } from "ng-zorro-antd/input";
import { NzPopoverModule } from "ng-zorro-antd/popover";
import { NzListModule } from "ng-zorro-antd/list";
import { NzCalendarModule } from "ng-zorro-antd/calendar";
import { NzMenuModule } from "ng-zorro-antd/menu";
import { NzMessageModule } from "ng-zorro-antd/message";
import { NzModalModule } from "ng-zorro-antd/modal";
import { NzTableModule } from "ng-zorro-antd/table";
import { NzToolTipModule } from "ng-zorro-antd/tooltip";
import { NzCheckboxModule } from "ng-zorro-antd/checkbox";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzSliderModule } from "ng-zorro-antd/slider";
import { NzSpaceModule } from "ng-zorro-antd/space";
import { NzBadgeModule } from "ng-zorro-antd/badge";
import { NzUploadModule } from "ng-zorro-antd/upload";
import { NzNoAnimationModule } from "ng-zorro-antd/core/no-animation";
import { FileUploadModule } from "ng2-file-upload";
import { NgxJsonViewerModule } from "ngx-json-viewer";
import { ColorPickerModule } from "ngx-color-picker";
import { environment } from "../environments/environment";
import { AppRoutingModule } from "./app-routing.module";
import { AppComponent } from "./app.component";
import { CustomNgMaterialModule } from "./common/custom-ng-material.module";
import { ArrayTypeComponent } from "./common/formly/array.type";
import { TEXERA_FORMLY_CONFIG } from "./common/formly/formly-config";
import { MultiSchemaTypeComponent } from "./common/formly/multischema.type";
import { NullTypeComponent } from "./common/formly/null.type";
import { ObjectTypeComponent } from "./common/formly/object.type";
import { UserFileUploadService } from "./dashboard/service/user-file/user-file-upload.service";
import { UserFileService } from "./dashboard/service/user-file/user-file.service";
import { UserService } from "./common/service/user/user.service";
import { DashboardComponent } from "./dashboard/component/dashboard.component";
import { FeatureBarComponent } from "./dashboard/component/feature-bar/feature-bar.component";
import { FeatureContainerComponent } from "./dashboard/component/feature-container/feature-container.component";
import { NgbdModalAddWorkflowComponent } from "./dashboard/component/feature-container/saved-workflow-section/ngbd-modal-add-workflow/ngbd-modal-add-workflow.component";
import { SavedWorkflowSectionComponent } from "./dashboard/component/feature-container/saved-workflow-section/saved-workflow-section.component";
import { NgbdModalFileAddComponent } from "./dashboard/component/feature-container/user-file-section/ngbd-modal-file-add/ngbd-modal-file-add.component";
import { UserFileSectionComponent } from "./dashboard/component/feature-container/user-file-section/user-file-section.component";
import { TopBarComponent } from "./dashboard/component/top-bar/top-bar.component";
import { UserIconComponent } from "./dashboard/component/top-bar/user-icon/user-icon.component";
import { UserAvatarComponent } from "./dashboard/component/user-avatar/user-avatar.component";
import { UserLoginModalComponent } from "./dashboard/component/top-bar/user-icon/user-login/user-login-modal.component";
import { CodeEditorDialogComponent } from "./workspace/component/code-editor-dialog/code-editor-dialog.component";
import { CodeareaCustomTemplateComponent } from "./workspace/component/codearea-custom-template/codearea-custom-template.component";
import { MiniMapComponent } from "./workspace/component/workflow-editor/mini-map/mini-map.component";
import { NavigationComponent } from "./workspace/component/navigation/navigation.component";
import { OperatorLabelComponent } from "./workspace/component/operator-panel/operator-label/operator-label.component";
import { OperatorPanelComponent } from "./workspace/component/operator-panel/operator-panel.component";
import { PropertyEditorComponent } from "./workspace/component/property-editor/property-editor.component";
import { TypeCastingDisplayComponent } from "./workspace/component/property-editor/typecasting-display/type-casting-display.component";
import { ResultPanelToggleComponent } from "./workspace/component/result-panel-toggle/result-panel-toggle.component";
import { ResultPanelComponent } from "./workspace/component/result-panel/result-panel.component";
import { VisualizationFrameContentComponent } from "./workspace/component/visualization-panel-content/visualization-frame-content.component";
import { VisualizationFrameComponent } from "./workspace/component/result-panel/visualization-frame/visualization-frame.component";
import { WorkflowEditorComponent } from "./workspace/component/workflow-editor/workflow-editor.component";
import { WorkspaceComponent } from "./workspace/component/workspace.component";
import { GoogleApiModule, NG_GAPI_CONFIG } from "ng-gapi";
import { NgbdModalWorkflowShareAccessComponent } from "./dashboard/component/feature-container/saved-workflow-section/ngbd-modal-share-access/ngbd-modal-workflow-share-access.component";
import { NgbdModalUserFileShareAccessComponent } from "./dashboard/component/feature-container/user-file-section/ngbd-modal-file-share-access/ngbd-modal-user-file-share-access.component";
import { NzCardModule } from "ng-zorro-antd/card";
import { NzStatisticModule } from "ng-zorro-antd/statistic";
import { NzTagModule } from "ng-zorro-antd/tag";
import { NzAvatarModule } from "ng-zorro-antd/avatar";
import { BlobErrorHttpInterceptor } from "./common/service/blob-error-http-interceptor.service";
import { ConsoleFrameComponent } from "./workspace/component/result-panel/console-frame/console-frame.component";
import { ResultTableFrameComponent } from "./workspace/component/result-panel/result-table-frame/result-table-frame.component";
import { DynamicModule } from "ng-dynamic-component";
import { RowModalComponent } from "./workspace/component/result-panel/result-panel-modal.component";
import { MonacoEditorModule } from "ngx-monaco-editor";
import { OperatorPropertyEditFrameComponent } from "./workspace/component/property-editor/operator-property-edit-frame/operator-property-edit-frame.component";
import { BreakpointPropertyEditFrameComponent } from "./workspace/component/property-editor/breakpoint-property-edit-frame/breakpoint-property-edit-frame.component";
import { NotificationComponent } from "./common/component/notification/notification/notification.component";
import { DebuggerFrameComponent } from "./workspace/component/result-panel/debugger-frame/debugger-frame.component";
import { NzTabsModule } from "ng-zorro-antd/tabs";
import { NzTreeViewModule } from "ng-zorro-antd/tree-view";
import { VersionsListDisplayComponent } from "./workspace/component/property-editor/versions-display/versions-display.component";
import { NzPaginationModule } from "ng-zorro-antd/pagination";
import { JwtModule } from "@auth0/angular-jwt";
import { AuthService } from "./common/service/user/auth.service";
import { UserProjectListComponent } from "./dashboard/component/feature-container/user-project-list/user-project-list.component";
import { UserProjectSectionComponent } from "./dashboard/component/feature-container/user-project-list/user-project-section/user-project-section.component";
import { NgbdModalAddProjectWorkflowComponent } from "./dashboard/component/feature-container/user-project-list/user-project-section/ngbd-modal-add-project-workflow/ngbd-modal-add-project-workflow.component";
import { NgbdModalRemoveProjectWorkflowComponent } from "./dashboard/component/feature-container/user-project-list/user-project-section/ngbd-modal-remove-project-workflow/ngbd-modal-remove-project-workflow.component";
import { NgbdModalAddProjectFileComponent } from "./dashboard/component/feature-container/user-project-list/user-project-section/ngbd-modal-add-project-file/ngbd-modal-add-project-file.component";
import { NgbdModalRemoveProjectFileComponent } from "./dashboard/component/feature-container/user-project-list/user-project-section/ngbd-modal-remove-project-file/ngbd-modal-remove-project-file.component";
import { PresetWrapperComponent } from "./common/formly/preset-wrapper/preset-wrapper.component";
import { NzModalCommentBoxComponent } from "./workspace/component/workflow-editor/comment-box-modal/nz-modal-comment-box.component";
import { NzCommentModule } from "ng-zorro-antd/comment";
import { NgbdModalWorkflowExecutionsComponent } from "./dashboard/component/feature-container/saved-workflow-section/ngbd-modal-workflow-executions/ngbd-modal-workflow-executions.component";
import { DeletePromptComponent } from "./dashboard/component/delete-prompt/delete-prompt.component";
import { ContextMenuComponent } from "./workspace/component/workflow-editor/context-menu/context-menu/context-menu.component";
import { NzImageModule } from "ng-zorro-antd/image";
import { CoeditorUserIconComponent } from "./workspace/component/navigation/coeditor-user-icon/coeditor-user-icon/coeditor-user-icon.component";
import { InputAutoCompleteComponent } from "./workspace/component/input-autocomplete/input-autocomplete.component";
import { CollabWrapperComponent } from "./common/formly/collab-wrapper/collab-wrapper/collab-wrapper.component";
import { NzSwitchModule } from "ng-zorro-antd/switch";

registerLocaleData(en);

@NgModule({
  declarations: [
    AppComponent,
    WorkspaceComponent,
    NavigationComponent,
    OperatorPanelComponent,
    PropertyEditorComponent,
    VersionsListDisplayComponent,
    WorkflowEditorComponent,
    ResultPanelComponent,
    OperatorLabelComponent,
    DashboardComponent,
    TopBarComponent,
    UserIconComponent,
    UserAvatarComponent,
    FeatureBarComponent,
    FeatureContainerComponent,
    SavedWorkflowSectionComponent,
    NgbdModalAddWorkflowComponent,
    UserLoginModalComponent,
    UserFileSectionComponent,
    NgbdModalFileAddComponent,
    RowModalComponent,
    OperatorLabelComponent,
    MiniMapComponent,
    ResultPanelToggleComponent,
    ArrayTypeComponent,
    ObjectTypeComponent,
    PresetWrapperComponent,
    MultiSchemaTypeComponent,
    NullTypeComponent,
    VisualizationFrameComponent,
    VisualizationFrameContentComponent,
    CodeareaCustomTemplateComponent,
    CodeEditorDialogComponent,
    TypeCastingDisplayComponent,
    NgbdModalWorkflowShareAccessComponent,
    NgbdModalWorkflowExecutionsComponent,
    NgbdModalUserFileShareAccessComponent,
    ConsoleFrameComponent,
    ResultTableFrameComponent,
    OperatorPropertyEditFrameComponent,
    BreakpointPropertyEditFrameComponent,
    NotificationComponent,
    ResultTableFrameComponent,
    OperatorPropertyEditFrameComponent,
    BreakpointPropertyEditFrameComponent,
    DebuggerFrameComponent,
    UserProjectListComponent,
    UserProjectSectionComponent,
    NgbdModalAddProjectWorkflowComponent,
    NgbdModalRemoveProjectWorkflowComponent,
    NgbdModalAddProjectFileComponent,
    NgbdModalRemoveProjectFileComponent,
    NzModalCommentBoxComponent,
    DeletePromptComponent,
    ContextMenuComponent,
    CoeditorUserIconComponent,
    InputAutoCompleteComponent,
    CollabWrapperComponent,
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    HttpClientModule,
    JwtModule.forRoot({
      config: {
        tokenGetter: AuthService.getAccessToken,
        skipWhenExpired: false,
        throwNoTokenError: false,
      },
    }),
    MatTooltipModule,
    CustomNgMaterialModule,
    BrowserAnimationsModule,
    NgbModule,
    NgbPopoverModule,
    RouterModule.forRoot([]),
    FileUploadModule,
    FormsModule,
    ReactiveFormsModule,
    FormlyModule.forRoot(TEXERA_FORMLY_CONFIG),
    FormlyMaterialModule,
    FormlyMatDatepickerModule,
    GoogleApiModule.forRoot({
      provide: NG_GAPI_CONFIG,
      useValue: {
        client_id: environment.google.clientID,
      },
    }),
    NzDatePickerModule,
    NzDropDownModule,
    NzButtonModule,
    NzAutocompleteModule,
    NzIconModule,
    NzFormModule,
    NzImageModule,
    NzListModule,
    NzInputModule,
    NzPopoverModule,
    NzCalendarModule,
    NzMenuModule,
    NzMessageModule,
    NzCollapseModule,
    NzToolTipModule,
    NzCheckboxModule,
    NzTableModule,
    NzModalModule,
    NzSelectModule,
    NzSliderModule,
    NzSpaceModule,
    NzBadgeModule,
    NzUploadModule,
    NzNoAnimationModule,
    NgxJsonViewerModule,
    MatDialogModule,
    NzCardModule,
    NzStatisticModule,
    NzTagModule,
    NzAvatarModule,
    DynamicModule,
    MonacoEditorModule.forRoot(),
    NzTabsModule,
    NzTreeViewModule,
    NzPaginationModule,
    NzCommentModule,
    ColorPickerModule,
    NgPipesModule,
    NzSwitchModule,
  ],
  providers: [
    DatePipe,
    UserService,
    UserFileService,
    UserFileUploadService,
    { provide: NZ_I18N, useValue: en_US },
    {
      provide: HTTP_INTERCEPTORS,
      useClass: BlobErrorHttpInterceptor,
      multi: true,
    },
  ],
  bootstrap: [AppComponent],
})
export class AppModule {}
