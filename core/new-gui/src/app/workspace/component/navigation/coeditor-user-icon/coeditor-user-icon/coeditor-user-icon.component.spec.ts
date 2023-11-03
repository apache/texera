import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { CoeditorUserIconComponent } from "./coeditor-user-icon.component";
import { CoeditorPresenceService } from "../../../../service/workflow-graph/model/coeditor-presence.service";
import { WorkflowActionService } from "../../../../service/workflow-graph/model/workflow-action.service";
import { HttpClient } from "@angular/common/http";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzDropdownMenuComponent, NzDropDownModule } from "ng-zorro-antd/dropdown";

describe("CoeditorUserIconComponent", () => {
  let component: CoeditorUserIconComponent;
  let fixture: ComponentFixture<CoeditorUserIconComponent>;
  let coeditorPresenceService: CoeditorPresenceService;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, NzDropDownModule],
      declarations: [CoeditorUserIconComponent],
      providers: [WorkflowActionService, CoeditorPresenceService, HttpClient, NzDropdownMenuComponent],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CoeditorUserIconComponent);
    component = fixture.componentInstance;
    coeditorPresenceService = TestBed.inject(CoeditorPresenceService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
