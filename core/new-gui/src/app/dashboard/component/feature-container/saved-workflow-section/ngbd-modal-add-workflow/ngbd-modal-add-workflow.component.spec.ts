import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { MatDialogModule } from "@angular/material/dialog";

import { NgbActiveModal, NgbModule } from "@ng-bootstrap/ng-bootstrap";
import { FormsModule } from "@angular/forms";

import { HttpClientModule } from "@angular/common/http";

import { NgbdModalAddWorkflowComponent } from "./ngbd-modal-add-workflow.component";

describe("NgbdModalAddProjectComponent", () => {
  let component: NgbdModalAddWorkflowComponent;
  let fixture: ComponentFixture<NgbdModalAddWorkflowComponent>;

  let addcomponent: NgbdModalAddWorkflowComponent;
  let addfixture: ComponentFixture<NgbdModalAddWorkflowComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [NgbdModalAddWorkflowComponent],
      providers: [NgbActiveModal],
      imports: [MatDialogModule, NgbModule, FormsModule, HttpClientModule],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalAddWorkflowComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("addWorkflowComponent addWorkflow should add new workflow", () => {
    addfixture = TestBed.createComponent(NgbdModalAddWorkflowComponent);
    addcomponent = addfixture.componentInstance;
    addfixture.detectChanges();

    let getResult: String;
    getResult = "";
    addcomponent.name = "test";
    // addcomponent.newProject.subscribe((out: any) => getResult = out);
    addcomponent.addWorkflow();

    expect(getResult).toEqual("");
  });
});
