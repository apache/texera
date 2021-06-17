import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbActiveModal, NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';

import { NgbdModalResourceViewComponent } from './ngbd-modal-resource-view.component';
import { CustomNgMaterialModule } from '../../../../../common/custom-ng-material.module';
import { UserService } from '../../../../../common/service/user/user.service';
import { UserDictionaryService } from '../../../../../common/service/user/user-dictionary/user-dictionary.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { GoogleApiService, GoogleAuthService, NG_GAPI_CONFIG } from 'ng-gapi';
import { environment } from '../../../../../../environments/environment';

describe('NgbdModalResourceViewComponent', () => {
  let component: NgbdModalResourceViewComponent;
  let fixture: ComponentFixture<NgbdModalResourceViewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [NgbdModalResourceViewComponent],
      providers: [
        NgbActiveModal,
        UserService,
        UserDictionaryService,
        GoogleApiService,
        GoogleAuthService,
        {
          provide: NG_GAPI_CONFIG,
          useValue: { client_id: environment.google.clientID }
        }
      ],
      imports: [
        CustomNgMaterialModule,
        NgbModule,
        FormsModule,
        HttpClientTestingModule
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalResourceViewComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
