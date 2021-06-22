import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TopBarComponent } from './top-bar.component';
import { UserIconComponent } from './user-icon/user-icon.component';
import { RouterTestingModule } from '@angular/router/testing';

import { CustomNgMaterialModule } from '../../../common/custom-ng-material.module';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { UserService } from '../../../common/service/user/user.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { GoogleApiModule, GoogleApiService, GoogleAuthService, NG_GAPI_CONFIG } from 'ng-gapi';
import { environment } from '../../../../environments/environment';

describe('TopBarComponent', () => {
  let component: TopBarComponent;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [TopBarComponent, UserIconComponent],
      providers: [
        NgbModal,
        UserService,
        GoogleApiService,
        GoogleAuthService,
        {
          provide: NG_GAPI_CONFIG,
          useValue: { client_id: environment.google.clientID }
        }
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
    const fixture = TestBed.createComponent(TopBarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
