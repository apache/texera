import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbdModalUserAccountLoginComponent } from './ngbdmodal-user-account-login.component';
import { UserAccountService } from '../../../../service/user-account/user-account.service';
import { NgbActiveModal, NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { MatTabsModule, MatFormFieldModule, MatDialogModule, MatInputModule } from '@angular/material';
import { FormsModule } from '@angular/forms';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

describe('UserAccountLoginComponent', () => {
  let component: NgbdModalUserAccountLoginComponent;
  let fixture: ComponentFixture<NgbdModalUserAccountLoginComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgbdModalUserAccountLoginComponent ],
      providers: [
        NgbActiveModal,
        UserAccountService
      ],
      imports: [
        BrowserAnimationsModule,
        HttpClientTestingModule,
        MatTabsModule,
        MatFormFieldModule,
        MatInputModule,
        NgbModule,
        FormsModule,
        MatDialogModule
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalUserAccountLoginComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
