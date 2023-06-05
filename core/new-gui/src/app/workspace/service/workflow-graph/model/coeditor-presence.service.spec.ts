import { TestBed } from "@angular/core/testing";

import { CoeditorPresenceService } from "./coeditor-presence.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzDropdownMenuComponent, NzDropDownModule } from "ng-zorro-antd/dropdown";
import { CoeditorUserIconComponent } from "../../../component/navigation/coeditor-user-icon/coeditor-user-icon/coeditor-user-icon.component";
import { WorkflowActionService } from "./workflow-action.service";
import { HttpClient } from "@angular/common/http";
import { RouterTestingModule } from "@angular/router/testing";

describe("CoeditorPresenceService", () => {
  let service: CoeditorPresenceService;
  let workflowActionService: WorkflowActionService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [RouterTestingModule, HttpClientTestingModule, NzDropDownModule],
      declarations: [CoeditorUserIconComponent],
      providers: [WorkflowActionService, CoeditorPresenceService, HttpClient, NzDropdownMenuComponent],
    });
    service = TestBed.inject(CoeditorPresenceService);
    workflowActionService = TestBed.inject(WorkflowActionService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
