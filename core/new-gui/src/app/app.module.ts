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
import { AppRoutingModule } from "./app-routing.module";
import { AppComponent } from "./app.component";
import { CustomNgMaterialModule } from "./common/custom-ng-material.module";
import { ArrayTypeComponent } from "./common/formly/array.type";
import { TEXERA_FORMLY_CONFIG } from "./common/formly/formly-config";
import { MultiSchemaTypeComponent } from "./common/formly/multischema.type";
import { NullTypeComponent } from "./common/formly/null.type";
import { ObjectTypeComponent } from "./common/formly/object.type";
import { UserFileUploadService } from "./dashboard/user/service/user-file/user-file-upload.service";
import { UserFileService } from "./dashboard/user/service/user-file/user-file.service";
import { UserService } from "./common/service/user/user.service";
import { DashboardComponent } from "./dashboard/user/component/dashboard.component";
import { UserWorkflowComponent } from "./dashboard/user/component/user-workflow/user-workflow.component";
import { ShareAccessComponent } from "./dashboard/user/component/share-access/share-access.component";
import { NgbdModalWorkflowExecutionsComponent } from "./dashboard/user/component/user-workflow/ngbd-modal-workflow-executions/ngbd-modal-workflow-executions.component";
import { NgbdModalFileAddComponent } from "./dashboard/user/component/user-file/ngbd-modal-file-add/ngbd-modal-file-add.component";
import { UserFileComponent } from "./dashboard/user/component/user-file/user-file.component";
import { UserQuotaComponent } from "./dashboard/user/component/user-quota/user-quota.component";
import { UserIconComponent } from "./dashboard/user/component/user-icon/user-icon.component";
import { UserAvatarComponent } from "./dashboard/user/component/user-avatar/user-avatar.component";
import { CodeEditorDialogComponent } from "./workspace/component/code-editor-dialog/code-editor-dialog.component";
import { CodeareaCustomTemplateComponent } from "./workspace/component/codearea-custom-template/codearea-custom-template.component";
import { MiniMapComponent } from "./workspace/component/workflow-editor/mini-map/mini-map.component";
import { MenuComponent } from "./workspace/component/menu/menu.component";
import { OperatorLabelComponent } from "./workspace/component/left-panel/operator-menu/operator-label/operator-label.component";
import { OperatorMenuComponent } from "./workspace/component/left-panel/operator-menu/operator-menu.component";
import { PropertyEditorComponent } from "./workspace/component/property-editor/property-editor.component";
import { TypeCastingDisplayComponent } from "./workspace/component/property-editor/typecasting-display/type-casting-display.component";
import { ResultPanelToggleComponent } from "./workspace/component/result-panel-toggle/result-panel-toggle.component";
import { ResultPanelComponent } from "./workspace/component/result-panel/result-panel.component";
import { VisualizationFrameContentComponent } from "./workspace/component/visualization-panel-content/visualization-frame-content.component";
import { VisualizationFrameComponent } from "./workspace/component/result-panel/visualization-frame/visualization-frame.component";
import { WorkflowEditorComponent } from "./workspace/component/workflow-editor/workflow-editor.component";
import { WorkspaceComponent } from "./workspace/component/workspace.component";
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
import { NzTabsModule } from "ng-zorro-antd/tabs";
import { NzTreeViewModule } from "ng-zorro-antd/tree-view";
import { VersionsListComponent } from "./workspace/component/left-panel/versions-list/versions-list.component";
import { NzPaginationModule } from "ng-zorro-antd/pagination";
import { JwtModule } from "@auth0/angular-jwt";
import { AuthService } from "./common/service/user/auth.service";
import { UserProjectComponent } from "./dashboard/user/component/user-project/user-project.component";
import { UserProjectSectionComponent } from "./dashboard/user/component/user-project/user-project-section/user-project-section.component";
import { NgbdModalAddProjectWorkflowComponent } from "./dashboard/user/component/user-project/user-project-section/ngbd-modal-add-project-workflow/ngbd-modal-add-project-workflow.component";
import { NgbdModalRemoveProjectWorkflowComponent } from "./dashboard/user/component/user-project/user-project-section/ngbd-modal-remove-project-workflow/ngbd-modal-remove-project-workflow.component";
import { NgbdModalAddProjectFileComponent } from "./dashboard/user/component/user-project/user-project-section/ngbd-modal-add-project-file/ngbd-modal-add-project-file.component";
import { NgbdModalRemoveProjectFileComponent } from "./dashboard/user/component/user-project/user-project-section/ngbd-modal-remove-project-file/ngbd-modal-remove-project-file.component";
import { PresetWrapperComponent } from "./common/formly/preset-wrapper/preset-wrapper.component";
import { NzModalCommentBoxComponent } from "./workspace/component/workflow-editor/comment-box-modal/nz-modal-comment-box.component";
import { NzCommentModule } from "ng-zorro-antd/comment";
import { AdminUserComponent } from "./dashboard/admin/component/user/admin-user.component";
import { AdminExecutionComponent } from "./dashboard/admin/component/execution/admin-execution.component";
import { NzPopconfirmModule } from "ng-zorro-antd/popconfirm";
import { AdminGuardService } from "./dashboard/admin/service/admin-guard.service";
import { ContextMenuComponent } from "./workspace/component/workflow-editor/context-menu/context-menu/context-menu.component";
import { NzImageModule } from "ng-zorro-antd/image";
import { CoeditorUserIconComponent } from "./workspace/component/menu/coeditor-user-icon/coeditor-user-icon.component";
import { InputAutoCompleteComponent } from "./workspace/component/input-autocomplete/input-autocomplete.component";
import { CollabWrapperComponent } from "./common/formly/collab-wrapper/collab-wrapper/collab-wrapper.component";
import { NzSwitchModule } from "ng-zorro-antd/switch";
import { HomeComponent } from "./home/component/home.component";
import { NzLayoutModule } from "ng-zorro-antd/layout";
import { AuthGuardService } from "./common/service/user/auth-guard.service";
import { LocalLoginComponent } from "./home/component/login/local-login/local-login.component";
import { MarkdownModule } from "ngx-markdown";
import { FileSaverService } from "./dashboard/user/service/user-file/file-saver.service";
import { DragDropModule } from "@angular/cdk/drag-drop";
import { UserWorkflowListItemComponent } from "./dashboard/user/component/user-workflow/user-workflow-list-item/user-workflow-list-item.component";
import { UserProjectListItemComponent } from "./dashboard/user/component/user-project/user-project-list-item/user-project-list-item.component";
import { SortButtonComponent } from "./dashboard/user/component/sort-button/sort-button.component";
import { FiltersComponent } from "./dashboard/user/component/filters/filters.component";
import { FiltersInstructionsComponent } from "./dashboard/user/component/filters-instructions/filters-instructions.component";
import { UserFileListItemComponent } from "./dashboard/user/component/user-file/user-file-list-item/user-file-list-item.component";
import { SearchComponent } from "./dashboard/user/component/search/search.component";
import { SearchResultsComponent } from "./dashboard/user/component/search-results/search-results.component";
import { PortPropertyEditFrameComponent } from "./workspace/component/property-editor/port-property-edit-frame/port-property-edit-frame.component";
import { GmailComponent } from "./dashboard/admin/component/gmail/gmail.component";
import { PublicProjectComponent } from "./dashboard/user/component/user-project/public-project/public-project.component";
import { FlarumComponent } from "./dashboard/user/component/flarum/flarum.component";
import { NzAlertModule } from "ng-zorro-antd/alert";
import { LeftPanelComponent } from "./workspace/component/left-panel/left-panel.component";
import { ErrorFrameComponent } from "./workspace/component/result-panel/error-frame/error-frame.component";
import { NzResizableModule } from "ng-zorro-antd/resizable";

registerLocaleData(en);

@NgModule({
  declarations: [
    GmailComponent,
    PublicProjectComponent,
    AppComponent,
    WorkspaceComponent,
    MenuComponent,
    OperatorMenuComponent,
    PropertyEditorComponent,
    VersionsListComponent,
    WorkflowEditorComponent,
    ResultPanelComponent,
    OperatorLabelComponent,
    DashboardComponent,
    AdminUserComponent,
    AdminExecutionComponent,
    UserIconComponent,
    UserAvatarComponent,
    LocalLoginComponent,
    UserWorkflowComponent,
    UserFileComponent,
    UserQuotaComponent,
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
    ShareAccessComponent,
    NgbdModalWorkflowExecutionsComponent,
    ConsoleFrameComponent,
    ErrorFrameComponent,
    ResultTableFrameComponent,
    OperatorPropertyEditFrameComponent,
    BreakpointPropertyEditFrameComponent,
    NotificationComponent,
    ResultTableFrameComponent,
    OperatorPropertyEditFrameComponent,
    BreakpointPropertyEditFrameComponent,
    UserProjectComponent,
    UserProjectSectionComponent,
    NgbdModalAddProjectWorkflowComponent,
    NgbdModalRemoveProjectWorkflowComponent,
    NgbdModalAddProjectFileComponent,
    NgbdModalRemoveProjectFileComponent,
    NzModalCommentBoxComponent,
    LeftPanelComponent,
    LocalLoginComponent,
    ContextMenuComponent,
    CoeditorUserIconComponent,
    InputAutoCompleteComponent,
    CollabWrapperComponent,
    HomeComponent,
    UserWorkflowListItemComponent,
    UserProjectListItemComponent,
    SortButtonComponent,
    FiltersComponent,
    FiltersInstructionsComponent,
    UserFileListItemComponent,
    SearchComponent,
    SearchResultsComponent,
    PortPropertyEditFrameComponent,
    FlarumComponent,
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
    NzPopconfirmModule,
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
    NzLayoutModule,
    MarkdownModule.forRoot(),
    DragDropModule,
    NzAlertModule,
    NzResizableModule,
  ],
  providers: [
    AuthGuardService,
    AdminGuardService,
    DatePipe,
    UserService,
    UserFileService,
    UserFileUploadService,
    FileSaverService,
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
