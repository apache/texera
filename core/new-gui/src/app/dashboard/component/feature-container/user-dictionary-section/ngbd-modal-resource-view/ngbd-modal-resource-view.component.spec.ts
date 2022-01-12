import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { NgbActiveModal, NgbModule } from "@ng-bootstrap/ng-bootstrap";
import { FormsModule } from "@angular/forms";

import { NgbdModalResourceViewComponent } from "./ngbd-modal-resource-view.component";
import { CustomNgMaterialModule } from "../../../../../common/custom-ng-material.module";
import { UserService } from "../../../../../common/service/user/user.service";
import { UserDictionaryService } from "../../../../service/user-dictionary/user-dictionary.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { StubUserService } from "../../../../../common/service/user/stub-user.service";

describe("NgbdModalResourceViewComponent", () => {
  let component: NgbdModalResourceViewComponent;
  let fixture: ComponentFixture<NgbdModalResourceViewComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [NgbdModalResourceViewComponent],
        providers: [NgbActiveModal, { provide: UserService, useClass: StubUserService }, UserDictionaryService],
        imports: [CustomNgMaterialModule, NgbModule, FormsModule, HttpClientTestingModule],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalResourceViewComponent);
    component = fixture.componentInstance;
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
