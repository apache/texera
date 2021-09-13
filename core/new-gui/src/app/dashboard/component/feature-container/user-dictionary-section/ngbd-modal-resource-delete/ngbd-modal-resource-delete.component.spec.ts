import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { CustomNgMaterialModule } from "../../../../../common/custom-ng-material.module";

import { NgbModule, NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { FormsModule } from "@angular/forms";

import { HttpClientModule } from "@angular/common/http";
import { NgbdModalResourceDeleteComponent } from "./ngbd-modal-resource-delete.component";

describe("NgbdModalResourceDeleteComponent", () => {
  let component: NgbdModalResourceDeleteComponent;
  let fixture: ComponentFixture<NgbdModalResourceDeleteComponent>;

  let deletecomponent: NgbdModalResourceDeleteComponent;
  let deletefixture: ComponentFixture<NgbdModalResourceDeleteComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [NgbdModalResourceDeleteComponent],
        providers: [NgbActiveModal],
        imports: [CustomNgMaterialModule, NgbModule, FormsModule, HttpClientModule],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalResourceDeleteComponent);
    component = fixture.componentInstance;
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
