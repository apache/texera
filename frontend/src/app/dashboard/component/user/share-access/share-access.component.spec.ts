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

import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { of } from "rxjs";

import { NZ_MODAL_DATA, NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { NzMessageService } from "ng-zorro-antd/message";

import { ShareAccessComponent } from "./share-access.component";
import { ShareAccessService } from "../../../service/user/share-access/share-access.service";
import { UserService } from "../../../../common/service/user/user.service";
import { GmailService } from "../../../../common/service/gmail/gmail.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { DatasetService } from "../../../service/user/dataset/dataset.service";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";

describe("ShareAccessComponent.grantAccess", () => {
  let gmailSpy: { sendEmail: ReturnType<typeof vi.fn> };
  let accessServiceSpy: {
    grantAccess: ReturnType<typeof vi.fn>;
    getAccessList: ReturnType<typeof vi.fn>;
    getOwner: ReturnType<typeof vi.fn>;
  };

  function setupComponent(type: string, id: number): ShareAccessComponent {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, NoopAnimationsModule, ShareAccessComponent],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: { type, id, allOwners: [], inWorkspace: false } },
        { provide: ShareAccessService, useValue: accessServiceSpy },
        {
          provide: UserService,
          useValue: { getCurrentUser: () => ({ email: "me@example.com" }) },
        },
        { provide: GmailService, useValue: gmailSpy },
        { provide: NotificationService, useValue: { success: vi.fn(), error: vi.fn() } },
        { provide: NzMessageService, useValue: { error: vi.fn() } },
        { provide: NzModalService, useValue: {} },
        { provide: NzModalRef, useValue: { close: vi.fn() } },
        {
          provide: WorkflowPersistService,
          useValue: { getWorkflowIsPublished: vi.fn().mockReturnValue(of("Private")) },
        },
        {
          provide: DatasetService,
          useValue: {
            getDataset: vi.fn().mockReturnValue(of({ dataset: { isPublic: false } })),
          },
        },
        { provide: WorkflowActionService, useValue: {} },
      ],
    });
    const fixture = TestBed.createComponent(ShareAccessComponent);
    fixture.detectChanges();
    return fixture.componentInstance;
  }

  beforeEach(() => {
    gmailSpy = { sendEmail: vi.fn() };
    accessServiceSpy = {
      grantAccess: vi.fn().mockReturnValue(of(null)),
      getAccessList: vi.fn().mockReturnValue(of([])),
      getOwner: vi.fn().mockReturnValue(of("owner@example.com")),
    };
  });

  function grantAndCaptureMessage(c: ShareAccessComponent): string {
    c.emailTags = ["to@example.com"];
    c.grantAccess();
    return gmailSpy.sendEmail.mock.calls[0][1] as string;
  }

  it("uses the workflow dashboard path when sharing a workflow", () => {
    const message = grantAndCaptureMessage(setupComponent("workflow", 11));
    expect(message).toContain("/dashboard/user/workflow/11");
  });

  it("uses the dataset dashboard path when sharing a dataset", () => {
    const message = grantAndCaptureMessage(setupComponent("dataset", 22));
    expect(message).toContain("/dashboard/user/dataset/22");
  });

  it("uses the project dashboard path when sharing a project", () => {
    const message = grantAndCaptureMessage(setupComponent("project", 33));
    expect(message).toContain("/dashboard/user/project/33");
  });

  it("omits the access URL when sharing a computing-unit", () => {
    const message = grantAndCaptureMessage(setupComponent("computing-unit", 44));
    expect(message).not.toContain("/dashboard/user/");
  });
});
