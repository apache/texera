import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbdModalUserLoginComponent } from './ngbdmodal-user-login.component';
import { UserService } from '../../../../../common/service/user/user.service';
import { NgbActiveModal, NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { MatDialogModule } from '@angular/material/dialog';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatTabsModule } from '@angular/material/tabs';
import { FormBuilder, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { StubUserService } from '../../../../../common/service/user/stub-user.service';

describe('UserLoginComponent', () => {
  let component: NgbdModalUserLoginComponent;
  let fixture: ComponentFixture<NgbdModalUserLoginComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [NgbdModalUserLoginComponent],
      providers: [
        NgbActiveModal,
        {provide: UserService, useClass: StubUserService},
        FormBuilder
      ],
      imports: [
        BrowserAnimationsModule,
        HttpClientTestingModule,
        MatTabsModule,
        MatFormFieldModule,
        MatInputModule,
        NgbModule,
        FormsModule,
        ReactiveFormsModule,
        MatDialogModule
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalUserLoginComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
