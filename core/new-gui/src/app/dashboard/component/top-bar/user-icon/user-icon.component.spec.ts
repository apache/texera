import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UserIconComponent } from './user-icon.component';
import { UserService } from '../../../../common/service/user/user.service';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('UserIconComponent', () => {
  let component: UserIconComponent;
  let fixture: ComponentFixture<UserIconComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [UserIconComponent],
      providers: [
        NgbModal,
        UserService
      ],
      imports: [
        HttpClientTestingModule
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UserIconComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
