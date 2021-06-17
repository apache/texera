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
import { GoogleApiModule, GoogleApiService, GoogleAuthService, NG_GAPI_CONFIG } from 'ng-gapi';
import { environment } from '../../../../../../environments/environment';

describe('UserLoginComponent', () => {
  let component: NgbdModalUserLoginComponent;
  let fixture: ComponentFixture<NgbdModalUserLoginComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [NgbdModalUserLoginComponent],
      providers: [
        NgbActiveModal,
        UserService,
        FormBuilder,
        GoogleApiService,
        GoogleAuthService,
        {
          provide: NG_GAPI_CONFIG,
          useValue: { client_id: environment.google.clientID }
        }
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
