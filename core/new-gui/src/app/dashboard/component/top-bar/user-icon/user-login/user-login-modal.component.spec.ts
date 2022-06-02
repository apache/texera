import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { UserLoginModalComponent } from "./user-login-modal.component";
import { UserService } from "../../../../../common/service/user/user.service";
import { MatDialogModule } from "@angular/material/dialog";
import { MatFormFieldModule } from "@angular/material/form-field";
import { MatInputModule } from "@angular/material/input";
import { MatTabsModule } from "@angular/material/tabs";
import { FormBuilder, FormsModule, ReactiveFormsModule } from "@angular/forms";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { StubUserService } from "../../../../../common/service/user/stub-user.service";
import { NzModalModule, NzModalRef, NzModalService } from "ng-zorro-antd/modal";

describe("UserLoginComponent", () => {
  let component: UserLoginModalComponent;
  let fixture: ComponentFixture<UserLoginModalComponent>;
  let nzModalRefSpy: jasmine.SpyObj<NzModalRef>;

  beforeEach(
    waitForAsync(() => {
      const nzModalRefSpyObj = jasmine.createSpyObj("NzModalRef", ["close"]);
      TestBed.configureTestingModule({
        declarations: [UserLoginModalComponent],
        providers: [
          { provide: NzModalRef, useValue: nzModalRefSpyObj },
          { provide: UserService, useClass: StubUserService },
          FormBuilder,
        ],
        imports: [
          BrowserAnimationsModule,
          HttpClientTestingModule,
          MatTabsModule,
          MatFormFieldModule,
          MatInputModule,
          NzModalModule,
          FormsModule,
          ReactiveFormsModule,
          MatDialogModule,
        ],
      }).compileComponents();
      nzModalRefSpy = TestBed.inject(NzModalRef) as jasmine.SpyObj<NzModalRef>;
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(UserLoginModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
