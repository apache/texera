/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { DatePipe, registerLocaleData } from "@angular/common";
import { HTTP_INTERCEPTORS, HttpClientModule } from "@angular/common/http";
import en from "@angular/common/locales/en";
import { NgModule, APP_INITIALIZER } from "@angular/core";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { BrowserModule } from "@angular/platform-browser";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { RouterModule } from "@angular/router";
import { FormlyModule } from "@ngx-formly/core";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzCollapseModule } from "ng-zorro-antd/collapse";
import { NzDatePickerModule } from "ng-zorro-antd/date-picker";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzFormModule } from "ng-zorro-antd/form";
import { NzAutocompleteModule } from "ng-zorro-antd/auto-complete";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzInputModule } from "ng-zorro-antd/input";
import { NzPopoverModule } from "ng-zorro-antd/popover";
import { NzListModule } from "ng-zorro-antd/list";
import { NzTableModule } from "ng-zorro-antd/table";
import { NzToolTipModule } from "ng-zorro-antd/tooltip";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzSpaceModule } from "ng-zorro-antd/space";
import { NzBadgeModule } from "ng-zorro-antd/badge";
import { NzUploadModule } from "ng-zorro-antd/upload";
import { NgxJsonViewerModule } from "ngx-json-viewer";
import { ColorPickerModule } from "ngx-color-picker";
import { AppRoutingModule } from "./app-routing.module";
import { AppComponent } from "./app.component";
import { ArrayTypeComponent } from "./common/formly/array.type";
import { TEXERA_FORMLY_CONFIG } from "./common/formly/formly-config";
import { MultiSchemaTypeComponent } from "./common/formly/multischema.type";
import { NullTypeComponent } from "./common/formly/null.type";
import { ObjectTypeComponent } from "./common/formly/object.type";
import { UserService } from "./common/service/user/user.service";
import { GuiConfigService } from "./common/service/gui-config.service";
import { DashboardComponent } from "./dashboard/component/dashboard.component";
import { UserWorkflowComponent } from "./dashboard/component/user/user-workflow/user-workflow.component";
import { ShareAccessComponent } from "./dashboard/component/user/share-access/share-access.component";
import { WorkflowExecutionHistoryComponent } from "./dashboard/component/user/user-workflow/ngbd-modal-workflow-executions/workflow-execution-history.component";
import { UserQuotaComponent } from "./dashboard/component/user/user-quota/user-quota.component";
import { UserIconComponent } from "./dashboard/component/user/user-icon/user-icon.component";
import { UserAvatarComponent } from "./dashboard/component/user/user-avatar/user-avatar.component";
import { CodeEditorComponent } from "./workspace/component/code-editor-dialog/code-editor.component";
import { AnnotationSuggestionComponent } from "./workspace/component/code-editor-dialog/annotation-suggestion.component";
import { CodeareaCustomTemplateComponent } from "./workspace/component/codearea-custom-template/codearea-custom-template.component";
import { MiniMapComponent } from "./workspace/component/workflow-editor/mini-map/mini-map.component";
import { MenuComponent } from "./workspace/component/menu/menu.component";
import { OperatorLabelComponent } from "./workspace/component/left-panel/operator-menu/operator-label/operator-label.component";
import { OperatorMenuComponent } from "./workspace/component/left-panel/operator-menu/operator-menu.component";
import { SettingsComponent } from "./workspace/component/left-panel/settings/settings.component";
import { PropertyEditorComponent } from "./workspace/component/property-editor/property-editor.component";
import { TypeCastingDisplayComponent } from "./workspace/component/property-editor/typecasting-display/type-casting-display.component";
import { ResultPanelComponent } from "./workspace/component/result-panel/result-panel.component";
import { VisualizationFrameContentComponent } from "./workspace/component/visualization-panel-content/visualization-frame-content.component";
import { WorkflowEditorComponent } from "./workspace/component/workflow-editor/workflow-editor.component";
import { WorkspaceComponent } from "./workspace/component/workspace.component";
import { NzCardModule } from "ng-zorro-antd/card";
import { NzTagModule } from "ng-zorro-antd/tag";
import { NzAvatarModule } from "ng-zorro-antd/avatar";
import { BlobErrorHttpInterceptor } from "./common/service/blob-error-http-interceptor.service";
import { ConsoleFrameComponent } from "./workspace/component/result-panel/console-frame/console-frame.component";
import { ResultTableFrameComponent } from "./workspace/component/result-panel/result-table-frame/result-table-frame.component";
import { RowModalComponent } from "./workspace/component/result-panel/result-panel-modal.component";
import { OperatorPropertyEditFrameComponent } from "./workspace/component/property-editor/operator-property-edit-frame/operator-property-edit-frame.component";
import { NzTabsModule } from "ng-zorro-antd/tabs";
import { VersionsListComponent } from "./workspace/component/left-panel/versions-list/versions-list.component";
import { NzPaginationModule } from "ng-zorro-antd/pagination";
import { JwtModule } from "@auth0/angular-jwt";
import { AuthService } from "./common/service/user/auth.service";
import { UserProjectComponent } from "./dashboard/component/user/user-project/user-project.component";
import { UserProjectSectionComponent } from "./dashboard/component/user/user-project/user-project-section/user-project-section.component";
import { NgbdModalAddProjectWorkflowComponent } from "./dashboard/component/user/user-project/user-project-section/ngbd-modal-add-project-workflow/ngbd-modal-add-project-workflow.component";
import { NgbdModalRemoveProjectWorkflowComponent } from "./dashboard/component/user/user-project/user-project-section/ngbd-modal-remove-project-workflow/ngbd-modal-remove-project-workflow.component";
import { PresetWrapperComponent } from "./common/formly/preset-wrapper/preset-wrapper.component";
import { NzModalCommentBoxComponent } from "./workspace/component/workflow-editor/comment-box-modal/nz-modal-comment-box.component";
import { NzCommentModule } from "ng-zorro-antd/comment";
import { AdminUserComponent } from "./dashboard/component/admin/user/admin-user.component";
import { AdminExecutionComponent } from "./dashboard/component/admin/execution/admin-execution.component";
import { NzPopconfirmModule } from "ng-zorro-antd/popconfirm";
import { AdminGuardService } from "./dashboard/service/admin/guard/admin-guard.service";
import { ContextMenuComponent } from "./workspace/component/workflow-editor/context-menu/context-menu/context-menu.component";
import { CoeditorUserIconComponent } from "./workspace/component/menu/coeditor-user-icon/coeditor-user-icon.component";
import { InputAutoCompleteComponent } from "./workspace/component/input-autocomplete/input-autocomplete.component";
import { CollabWrapperComponent } from "./common/formly/collab-wrapper/collab-wrapper/collab-wrapper.component";
import { NzSwitchModule } from "ng-zorro-antd/switch";
import { AboutComponent } from "./hub/component/about/about.component";
import { NzLayoutModule } from "ng-zorro-antd/layout";
import { AuthGuardService } from "./common/service/user/auth-guard.service";
import { LocalLoginComponent } from "./hub/component/about/local-login/local-login.component";
import { MarkdownModule } from "ngx-markdown";
import { FileSaverService } from "./dashboard/service/user/file/file-saver.service";
import { DragDropModule } from "@angular/cdk/drag-drop";
import { UserWorkflowListItemComponent } from "./dashboard/component/user/user-workflow/user-workflow-list-item/user-workflow-list-item.component";
import { UserProjectListItemComponent } from "./dashboard/component/user/user-project/user-project-list-item/user-project-list-item.component";
import { SortButtonComponent } from "./dashboard/component/user/sort-button/sort-button.component";
import { FiltersComponent } from "./dashboard/component/user/filters/filters.component";
import { FiltersInstructionsComponent } from "./dashboard/component/user/filters-instructions/filters-instructions.component";
import { SearchComponent } from "./dashboard/component/user/search/search.component";
import { SearchResultsComponent } from "./dashboard/component/user/search-results/search-results.component";
import { PortPropertyEditFrameComponent } from "./workspace/component/property-editor/port-property-edit-frame/port-property-edit-frame.component";
import { AdminGmailComponent } from "./dashboard/component/admin/gmail/admin-gmail.component";
import { PublicProjectComponent } from "./dashboard/component/user/user-project/public-project/public-project.component";
import { FormlyNgZorroAntdModule } from "@ngx-formly/ng-zorro-antd";
import { FlarumComponent } from "./dashboard/component/user/flarum/flarum.component";
import { NzAlertModule } from "ng-zorro-antd/alert";
import { LeftPanelComponent } from "./workspace/component/left-panel/left-panel.component";
import { ErrorFrameComponent } from "./workspace/component/result-panel/error-frame/error-frame.component";
import { NzResizableModule } from "ng-zorro-antd/resizable";
import { WorkflowRuntimeStatisticsComponent } from "./dashboard/component/user/user-workflow/ngbd-modal-workflow-executions/workflow-runtime-statistics/workflow-runtime-statistics.component";
import { TimeTravelComponent } from "./workspace/component/left-panel/time-travel/time-travel.component";
import { NzMessageModule } from "ng-zorro-antd/message";
import { NzModalModule } from "ng-zorro-antd/modal";
import { OverlayModule } from "@angular/cdk/overlay";
import { HighlightSearchTermsPipe } from "./dashboard/component/user/user-workflow/user-workflow-list-item/highlight-search-terms.pipe";
import { en_US, provideNzI18n } from "ng-zorro-antd/i18n";
import { FilesUploaderComponent } from "./dashboard/component/user/files-uploader/files-uploader.component";
import { UserDatasetComponent } from "./dashboard/component/user/user-dataset/user-dataset.component";
import { UserDatasetVersionCreatorComponent } from "./dashboard/component/user/user-dataset/user-dataset-explorer/user-dataset-version-creator/user-dataset-version-creator.component";
import { DatasetDetailComponent } from "./dashboard/component/user/user-dataset/user-dataset-explorer/dataset-detail.component";
import { UserDatasetVersionFiletreeComponent } from "./dashboard/component/user/user-dataset/user-dataset-explorer/user-dataset-version-filetree/user-dataset-version-filetree.component";
import { UserDatasetFileRendererComponent } from "./dashboard/component/user/user-dataset/user-dataset-explorer/user-dataset-file-renderer/user-dataset-file-renderer.component";
import { UserDatasetContributorEditor } from "./dashboard/component/user/user-dataset/user-dataset-explorer/user-dataset-contributor-editor/user-dataset-contributor-editor.component";
import { NzSpinModule } from "ng-zorro-antd/spin";
import { UserDatasetListItemComponent } from "./dashboard/component/user/user-dataset/user-dataset-list-item/user-dataset-list-item.component";
import { NgxFileDropModule } from "ngx-file-drop";
import { NzTreeModule } from "ng-zorro-antd/tree";
import { NzTreeViewModule } from "ng-zorro-antd/tree-view";
import { NzNoAnimationModule } from "ng-zorro-antd/core/no-animation";
import { TreeModule } from "@ali-hm/angular-tree-component";
import { FileSelectionComponent } from "./workspace/component/file-selection/file-selection.component";
import { ResultExportationComponent } from "./workspace/component/result-exportation/result-exportation.component";
import { ReportGenerationService } from "./workspace/service/report-generation/report-generation.service";
import { SearchBarComponent } from "./dashboard/component/user/search-bar/search-bar.component";
import { ListItemComponent } from "./dashboard/component/user/list-item/list-item.component";
import { HubComponent } from "./hub/component/hub.component";
import { HubWorkflowDetailComponent } from "./hub/component/workflow/detail/hub-workflow-detail.component";
import { LandingPageComponent } from "./hub/component/landing-page/landing-page.component";
import { BrowseSectionComponent } from "./hub/component/browse-section/browse-section.component";
import { BreakpointConditionInputComponent } from "./workspace/component/code-editor-dialog/breakpoint-condition-input/breakpoint-condition-input.component";
import { CodeDebuggerComponent } from "./workspace/component/code-editor-dialog/code-debugger.component";
import { GoogleAuthService } from "./common/service/user/google-auth.service";
import { SocialLoginModule, SocialAuthServiceConfig, GoogleSigninButtonModule } from "@abacritt/angularx-social-login";
import { GoogleLoginProvider } from "@abacritt/angularx-social-login";
import { lastValueFrom, firstValueFrom } from "rxjs";
import { HubSearchResultComponent } from "./hub/component/hub-search-result/hub-search-result.component";
import { UserDatasetStagedObjectsListComponent } from "./dashboard/component/user/user-dataset/user-dataset-explorer/user-dataset-staged-objects-list/user-dataset-staged-objects-list.component";
import { NzEmptyModule } from "ng-zorro-antd/empty";
import { NzDividerModule } from "ng-zorro-antd/divider";
import { NzProgressModule } from "ng-zorro-antd/progress";
import { ComputingUnitSelectionComponent } from "./workspace/component/power-button/computing-unit-selection.component";
import { NzSliderModule } from "ng-zorro-antd/slider";
import { AdminSettingsComponent } from "./dashboard/component/admin/settings/admin-settings.component";
import { catchError, of } from "rxjs";

registerLocaleData(en);

@NgModule({
  declarations: [
    AdminGmailComponent,
    PublicProjectComponent,
    AppComponent,
    WorkspaceComponent,
    MenuComponent,
    OperatorMenuComponent,
    SettingsComponent,
    PropertyEditorComponent,
    VersionsListComponent,
    TimeTravelComponent,
    WorkflowEditorComponent,
    ResultPanelComponent,
    ResultExportationComponent,
    OperatorLabelComponent,
    DashboardComponent,
    AdminUserComponent,
    AdminExecutionComponent,
    UserIconComponent,
    UserAvatarComponent,
    LocalLoginComponent,
    UserWorkflowComponent,
    UserQuotaComponent,
    RowModalComponent,
    OperatorLabelComponent,
    MiniMapComponent,
    ArrayTypeComponent,
    ObjectTypeComponent,
    PresetWrapperComponent,
    MultiSchemaTypeComponent,
    NullTypeComponent,
    VisualizationFrameContentComponent,
    CodeareaCustomTemplateComponent,
    CodeEditorComponent,
    AnnotationSuggestionComponent,
    TypeCastingDisplayComponent,
    ShareAccessComponent,
    WorkflowExecutionHistoryComponent,
    ConsoleFrameComponent,
    ErrorFrameComponent,
    ResultTableFrameComponent,
    OperatorPropertyEditFrameComponent,
    ResultTableFrameComponent,
    OperatorPropertyEditFrameComponent,
    UserProjectComponent,
    UserProjectSectionComponent,
    NgbdModalAddProjectWorkflowComponent,
    NgbdModalRemoveProjectWorkflowComponent,
    FilesUploaderComponent,
    UserDatasetComponent,
    UserDatasetVersionCreatorComponent,
    DatasetDetailComponent,
    UserDatasetVersionFiletreeComponent,
    UserDatasetListItemComponent,
    UserDatasetFileRendererComponent,
    UserDatasetStagedObjectsListComponent,
    UserDatasetContributorEditor,
    NzModalCommentBoxComponent,
    LeftPanelComponent,
    LocalLoginComponent,
    ContextMenuComponent,
    CoeditorUserIconComponent,
    InputAutoCompleteComponent,
    FileSelectionComponent,
    CollabWrapperComponent,
    AboutComponent,
    UserWorkflowListItemComponent,
    UserProjectListItemComponent,
    SortButtonComponent,
    FiltersComponent,
    FiltersInstructionsComponent,
    SearchComponent,
    SearchResultsComponent,
    PortPropertyEditFrameComponent,
    WorkflowRuntimeStatisticsComponent,
    FlarumComponent,
    HighlightSearchTermsPipe,
    SearchBarComponent,
    ListItemComponent,
    HubComponent,
    HubWorkflowDetailComponent,
    LandingPageComponent,
    BrowseSectionComponent,
    BreakpointConditionInputComponent,
    CodeDebuggerComponent,
    HubSearchResultComponent,
    ComputingUnitSelectionComponent,
    AdminSettingsComponent,
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
        disallowedRoutes: ["forum/api/users"],
      },
    }),
    BrowserAnimationsModule,
    RouterModule,
    FormsModule,
    ReactiveFormsModule,
    FormlyModule.forRoot(TEXERA_FORMLY_CONFIG),
    FormlyNgZorroAntdModule,
    OverlayModule,
    NzDatePickerModule,
    NzDropDownModule,
    NzButtonModule,
    NzAutocompleteModule,
    NzIconModule,
    NzFormModule,
    NzListModule,
    NzInputModule,
    NzPopoverModule,
    NzCollapseModule,
    NzToolTipModule,
    NzTableModule,
    NzSelectModule,
    NzSpaceModule,
    NzBadgeModule,
    NzUploadModule,
    NgxJsonViewerModule,
    NzMessageModule,
    NzModalModule,
    NzCardModule,
    NzTagModule,
    NzPopconfirmModule,
    NzAvatarModule,
    NzTabsModule,
    NzPaginationModule,
    NzCommentModule,
    ColorPickerModule,
    NzSwitchModule,
    NzLayoutModule,
    NzSliderModule,
    MarkdownModule.forRoot(),
    DragDropModule,
    NzAlertModule,
    NzResizableModule,
    NzSpinModule,
    NgxFileDropModule,
    NzTreeModule,
    NzTreeViewModule,
    NzNoAnimationModule,
    TreeModule,
    SocialLoginModule,
    GoogleSigninButtonModule,
    NzEmptyModule,
    NzDividerModule,
    NzProgressModule,
  ],
  providers: [
    provideNzI18n(en_US),
    AuthGuardService,
    AdminGuardService,
    DatePipe,
    UserService,
    GuiConfigService,
    FileSaverService,
    ReportGenerationService,
    {
      provide: HTTP_INTERCEPTORS,
      useClass: BlobErrorHttpInterceptor,
      multi: true,
    },
    {
      provide: "SocialAuthServiceConfig",
      useFactory: (googleAuthService: GoogleAuthService, userService: UserService) => {
        return lastValueFrom(googleAuthService.getClientId()).then(clientId => ({
          providers: [
            {
              id: GoogleLoginProvider.PROVIDER_ID,
              provider: new GoogleLoginProvider(clientId, { oneTapEnabled: !userService.isLogin() }),
            },
          ],
        })) as Promise<SocialAuthServiceConfig>;
      },
      deps: [GoogleAuthService, UserService],
    },
    {
      provide: APP_INITIALIZER,
      useFactory: (configService: GuiConfigService) => () =>
        firstValueFrom(
          configService.load().pipe(
            catchError((err: unknown) => {
              console.error("Failed to load GUI config during app init:", err);
              // swallow error so the app can still bootstrap; app.component.ts will show the error message as HTML.
              return of(null);
            })
          )
        ),
      deps: [GuiConfigService],
      multi: true,
    },
  ],
  bootstrap: [AppComponent],
})
export class AppModule {}
