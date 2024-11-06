import { ComponentFixture, TestBed } from "@angular/core/testing";
import { UserProjectListItemComponent } from "./user-project-list-item.component";

import { UserProjectService } from "../../../../service/user/project/user-project.service";
import { DashboardProject } from "../../../../type/dashboard-project.interface";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalService } from "ng-zorro-antd/modal";
import { StubUserService } from "../../../../../common/service/user/stub-user.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { HighlightSearchTermsPipe } from "../../user-workflow/user-workflow-list-item/highlight-search-terms.pipe";
import { NotificationService } from "../../../../../common/service/notification/notification.service";

describe("UserProjectListItemComponent", () => {
  let component: UserProjectListItemComponent;
  let fixture: ComponentFixture<UserProjectListItemComponent>;
  const januaryFirst1970 = 28800000; // 1970-01-01 in PST
  const testProject: DashboardProject = {
    color: null,
    creationTime: januaryFirst1970,
    description: "description",
    name: "project1",
    ownerId: 1,
    pid: 1,
    accessLevel: "WRITE",
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [UserProjectListItemComponent, HighlightSearchTermsPipe],
      providers: [
        NotificationService,
        UserProjectService,
        NzModalService,
        { provide: UserService, useClass: StubUserService },
      ],
      imports: [HttpClientTestingModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UserProjectListItemComponent);
    component = fixture.componentInstance;
    component.entry = testProject;
    component.editable = true;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
