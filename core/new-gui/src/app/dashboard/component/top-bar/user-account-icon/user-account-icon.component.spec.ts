import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UserAccountIconComponent } from './user-account-icon.component';
import { UserAccountService } from '../../../service/user-account/user-account.service';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('UserAccountIconComponent', () => {
  let component: UserAccountIconComponent;
  let fixture: ComponentFixture<UserAccountIconComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ UserAccountIconComponent ],
      providers: [
        NgbModal,
        UserAccountService
      ],
      imports: [
        HttpClientTestingModule
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UserAccountIconComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
