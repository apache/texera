import { ComponentFixture, TestBed } from "@angular/core/testing";
import { UserProjectListItemComponent } from "./user-project-list-item.component";
import { HttpClient, HttpHandler } from "@angular/common/http";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { UserProjectService } from "../../../service/user-project/user-project.service";
import { DashboardProject } from "../../../type/dashboard-project.interface";

describe("UserProjectListItemComponent", () => {
  let component: UserProjectListItemComponent;
  let fixture: ComponentFixture<UserProjectListItemComponent>;
  const januaryFirst1970 = 28800000; // 1970-01-01 in PST
  const testProject: DashboardProject = {
    color: null,
    creationTime: januaryFirst1970,
    description: "description",
    name: "project1",
    ownerID: 1,
    pid: 1,
    accessLevel: "WRITE",
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [UserProjectListItemComponent],
      providers: [HttpClient, HttpHandler, NotificationService, UserProjectService],
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
