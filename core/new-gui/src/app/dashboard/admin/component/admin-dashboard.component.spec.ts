import { ComponentFixture, inject, TestBed, waitForAsync } from "@angular/core/testing";
import { AdminDashboardComponent } from "./admin-dashboard.component";
import { AdminExecutionService } from "../service/admin-execution.service";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";

describe("AdminDashboardComponent", () => {
  let component: AdminDashboardComponent;
  let fixture: ComponentFixture<AdminDashboardComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [AdminDashboardComponent],
      providers: [NgbModal, AdminExecutionService],
      imports: [HttpClientTestingModule, NzDropDownModule],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminDashboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", inject([HttpTestingController], () => {
    expect(component).toBeTruthy();
  }));
});
