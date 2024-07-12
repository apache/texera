import { ComponentFixture, inject, TestBed, waitForAsync } from "@angular/core/testing";
import { AdminExecutionComponent } from "./admin-execution.component";
import { AdminExecutionService } from "../../../user/service/admin/execution/admin-execution.service";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzModalModule } from "ng-zorro-antd/modal";

describe("AdminDashboardComponent", () => {
  let component: AdminExecutionComponent;
  let fixture: ComponentFixture<AdminExecutionComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [AdminExecutionComponent],
      providers: [AdminExecutionService],
      imports: [HttpClientTestingModule, NzDropDownModule, NzModalModule],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminExecutionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", inject([HttpTestingController], () => {
    expect(component).toBeTruthy();
  }));
});
