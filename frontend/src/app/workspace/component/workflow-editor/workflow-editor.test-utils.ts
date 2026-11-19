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

/**
 * Shared TestBed configuration for WorkflowEditorComponent specs.
 *
 * The workflow editor has two spec files that drive the same component:
 *   - workflow-editor.component.spec.ts          (jsdom test target)
 *   - workflow-editor.component.browser.spec.ts  (test-browser target,
 *                                                 for JointJS event paths
 *                                                 that need real DOM/SVG)
 *
 * Both specs configure TestBed with the same set of imports and
 * providers. Exporting the two arrays here keeps them in lock-step so
 * the two TestBed setups don't drift over time.
 */

import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { TestBed } from "@angular/core/testing";
import { Provider } from "@angular/core";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { RouterTestingModule } from "@angular/router/testing";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";
import { NzContextMenuService, NzDropDownModule } from "ng-zorro-antd/dropdown";

import { WorkflowEditorComponent } from "./workflow-editor.component";
import { NzModalCommentBoxComponent } from "./comment-box-modal/nz-modal-comment-box.component";

import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { UserService } from "src/app/common/service/user/user.service";
import { StubUserService } from "src/app/common/service/user/stub-user.service";
import { commonTestProviders } from "../../../common/testing/test-utils";

export const workflowEditorTestImports = [
  RouterTestingModule,
  HttpClientTestingModule,
  NzModalModule,
  NzDropDownModule,
  NoopAnimationsModule,
  WorkflowEditorComponent,
  NzModalCommentBoxComponent,
];

export const workflowEditorTestProviders: Provider[] = [
  NzModalService,
  NzContextMenuService,
  { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
  { provide: UserService, useClass: StubUserService },
  ...commonTestProviders,
];

export function configureWorkflowEditor() {
  return TestBed.configureTestingModule({
    imports: workflowEditorTestImports,
    providers: [
      ...workflowEditorTestProviders,
      { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
    ],
  }).compileComponents();
}
