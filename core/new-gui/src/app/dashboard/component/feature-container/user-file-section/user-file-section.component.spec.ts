import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UserFileSectionComponent } from './user-file-section.component';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { UserFileService } from '../../../service/user-file/user-file.service';
import { UserAccountService } from '../../../service/user-account/user-account.service';

describe('UserFileSectionComponent', () => {
  let component: UserFileSectionComponent;
  let fixture: ComponentFixture<UserFileSectionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ UserFileSectionComponent ],
      providers: [
        NgbModal,
        UserFileService,
        UserAccountService
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UserFileSectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  // it('should create', () => {
  //   expect(component).toBeTruthy();
  // });
});
