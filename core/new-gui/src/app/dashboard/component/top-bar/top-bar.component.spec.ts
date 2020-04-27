import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TopBarComponent } from './top-bar.component';
import { UserAccountIconComponent } from './user-account-icon/user-account-icon.component';
import { RouterTestingModule } from '@angular/router/testing';

import { CustomNgMaterialModule } from '../../../common/custom-ng-material.module';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { UserAccountService } from '../../service/user-account/user-account.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('TopBarComponent', () => {
  let component: TopBarComponent;
  let fixture: ComponentFixture<TopBarComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TopBarComponent, UserAccountIconComponent ],
      providers: [
        NgbModal,
        UserAccountService
      ],
      imports: [
        HttpClientTestingModule,
        RouterTestingModule,
        CustomNgMaterialModule
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TopBarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
